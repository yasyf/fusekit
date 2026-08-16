import DaemonKit
import Foundation

/// CatalogBrokerChildError rejects malformed signed-broker child mode.
public enum CatalogBrokerChildError: Error, Equatable {
  case invalidArguments
}

/// CatalogBrokerChildMode is the exact fixed-app launch contract for one broker process.
public enum CatalogBrokerChildMode {
  /// requested recognizes only the current exact broker child argv, which
  /// names no socket path: the daemon reaches the child solely through the
  /// inherited spawn channel.
  public static func requested(arguments: [String]) throws -> Bool {
    let mode = "--fusekit-broker-child"
    let tail = Array(arguments.dropFirst())
    guard tail.contains(mode) else { return false }
    guard tail == [mode] else {
      throw CatalogBrokerChildError.invalidArguments
    }
    return true
  }
}

/// CatalogBroker runs domain control and the sealed App Group descriptor bridge.
public final class CatalogBroker: @unchecked Sendable {
  /// Configuration pins the signed App Group endpoint and client posture.
  public struct Configuration: Sendable {
    public let appGroupEndpoint: CatalogAppGroupEndpoint
    public let client: SocketClient.Configuration

    public init(
      appGroupEndpoint: CatalogAppGroupEndpoint,
      client: SocketClient.Configuration = .init()
    ) {
      self.appGroupEndpoint = appGroupEndpoint
      self.client = client
    }
  }

  private let daemon: SocketClient
  private let bridge: BrokerSocketBridge
  private let state: CatalogBrokerState

  public init(channel: SpawnedChannel, configuration: Configuration) async throws {
    daemon = try await SocketClient(
      connection: .spawned(channel),
      schema: FuseKitTransportProtocol.wireBuild,
      lane: .business,
      configuration: configuration.client
    )
    state = CatalogBrokerState(
      daemon: daemon,
      controller: CatalogDomainController()
    )
    bridge = try BrokerSocketBridge(
      container: configuration.appGroupEndpoint.container,
      socket: configuration.appGroupEndpoint.leaf,
      lifecycle: RuntimeClientConfiguration(
        connection: .spawned(channel),
        schema: FuseKitTransportProtocol.wireBuild,
        lane: .business,
        socket: configuration.client
      )
    )
  }

  /// run owns both the domain-control stream and sealed descriptor handoff bridge.
  public func run() async throws {
    do {
      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await self.bridge.run() }
        group.addTask { try await self.state.runBroker() }
        _ = try await group.next()
        group.cancelAll()
        await bridge.shutdown()
        await daemon.close()
        while try await group.next() != nil {}
      }
    } catch {
      await bridge.shutdown()
      await daemon.close()
      throw error
    }
  }

  /// runChildIfRequested runs the exact broker mode before normal app startup,
  /// refusing outright — never falling through to app startup — when the
  /// inherited spawn channel cannot be claimed and proven.
  public static func runChildIfRequested(
    arguments: [String] = ProcessInfo.processInfo.arguments,
    configuration: Configuration
  ) async throws -> Bool {
    guard try CatalogBrokerChildMode.requested(arguments: arguments) else {
      return false
    }
    let channel = try SpawnedChannel.claim()
    try await CatalogBroker(channel: channel, configuration: configuration).run()
    return true
  }
}

private actor CatalogBrokerState {
  private let daemon: SocketClient
  private let controller: CatalogDomainController
  private let encoder: JSONEncoder
  private let decoder = JSONDecoder()

  init(daemon: SocketClient, controller: CatalogDomainController) {
    self.daemon = daemon
    self.controller = controller
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  func runBroker() async throws {
    var instance: CatalogBrokerInstanceID?
    var cursor: UInt64 = 0
    while true {
      let poll = try await poll(instance: instance, cursor: cursor)
      guard poll.code == .ok, let bound = poll.instance else {
        throw CatalogTransportError.remote(poll.message)
      }
      instance = bound
      for command in poll.commands {
        let result = await controller.execute(command)
        let posted = try await postResult(instance: bound, result: result)
        guard posted.code == .ok else {
          throw CatalogTransportError.remote(posted.message)
        }
      }
      // nextCursor is 0 on an empty poll; only a delivered batch advances it, so
      // an idle re-poll keeps its place instead of rebinding from the start.
      if !poll.commands.isEmpty {
        cursor = poll.nextCursor
      }
    }
  }

  private func poll(
    instance: CatalogBrokerInstanceID?,
    cursor: UInt64
  ) async throws -> CatalogBrokerPollResponse {
    let request = try CatalogBrokerPollRequest(
      instance: instance, cursor: cursor, waitMillis: CatalogProtocol.maxPollWaitMillis
    )
    let body = try CatalogRequestEnvelope.encode(tenant: "", payload: encoder.encode(request))
    let terminal = try await daemon.call(
      operation: CatalogOperation.brokerPoll.rawValue, payload: body
    )
    return try Self.decodeTerminal(terminal, as: CatalogBrokerPollResponse.self, decoder: decoder)
  }

  private func postResult(
    instance: CatalogBrokerInstanceID,
    result: CatalogBrokerResult
  ) async throws -> CatalogPostBrokerResultResponse {
    let request = CatalogPostBrokerResultRequest(instance: instance, result: result)
    let body = try CatalogRequestEnvelope.encode(tenant: "", payload: encoder.encode(request))
    let terminal = try await daemon.call(
      operation: CatalogOperation.brokerResult.rawValue, payload: body
    )
    return try Self.decodeTerminal(
      terminal, as: CatalogPostBrokerResultResponse.self, decoder: decoder
    )
  }

  private static func decodeTerminal<Value: Decodable>(
    _ terminal: SocketTerminal,
    as type: Value.Type,
    decoder: JSONDecoder
  ) throws -> Value {
    if terminal.rejected {
      throw CatalogTransportError.rejected(terminal.reason ?? "request rejected")
    }
    if let error = terminal.error {
      throw CatalogTransportError.remote(error)
    }
    guard let payload = terminal.payload else { throw CatalogTransportError.missingPayload }
    return try decoder.decode(type, from: payload)
  }
}
