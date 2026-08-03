import DaemonKit
import Foundation

/// CatalogBrokerChildError rejects malformed or substituted signed-broker child mode.
public enum CatalogBrokerChildError: Error, Equatable {
  case invalidArguments
  case daemonSocketMismatch
}

/// CatalogBrokerChildMode is the exact fixed-app launch contract for one broker process.
public struct CatalogBrokerChildMode: Equatable, Sendable {
  public let daemonSocketPath: String

  /// parse recognizes only the current exact broker child argv.
  public static func parse(arguments: [String]) throws -> CatalogBrokerChildMode? {
    let mode = "--fusekit-broker-child"
    let socket = "--fusekit-daemon-socket"
    let tail = Array(arguments.dropFirst())
    guard tail.contains(mode) else { return nil }
    guard tail.count == 3, tail[0] == mode, tail[1] == socket else {
      throw CatalogBrokerChildError.invalidArguments
    }
    let path = tail[2]
    guard path.hasPrefix("/"), !path.contains("\0"),
          URL(fileURLWithPath: path).standardizedFileURL.path == path
    else {
      throw CatalogBrokerChildError.invalidArguments
    }
    return CatalogBrokerChildMode(daemonSocketPath: path)
  }
}

/// CatalogBroker runs domain control and the sealed App Group descriptor bridge.
public final class CatalogBroker: @unchecked Sendable {
  /// Configuration pins one daemon runtime and the signed App Group endpoint.
  public struct Configuration: Sendable {
    public let appGroupEndpoint: CatalogAppGroupEndpoint
    public let daemonSocketPath: String
    public let client: SocketClient.Configuration

    public init(
      appGroupEndpoint: CatalogAppGroupEndpoint,
      daemonSocketPath: String,
      client: SocketClient.Configuration = .init()
    ) {
      self.appGroupEndpoint = appGroupEndpoint
      self.daemonSocketPath = daemonSocketPath
      self.client = client
    }
  }

  private let daemon: SocketClient
  private let bridge: BrokerSocketBridge
  private let state: CatalogBrokerState

  public init(configuration: Configuration) async throws {
    daemon = try await SocketClient(
      path: configuration.daemonSocketPath,
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
        path: configuration.daemonSocketPath,
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

  /// runChildIfRequested runs the exact broker mode before normal app startup.
  public static func runChildIfRequested(
    arguments: [String] = ProcessInfo.processInfo.arguments,
    configuration: Configuration
  ) async throws -> Bool {
    guard let child = try CatalogBrokerChildMode.parse(arguments: arguments) else {
      return false
    }
    guard child.daemonSocketPath == configuration.daemonSocketPath else {
      throw CatalogBrokerChildError.daemonSocketMismatch
    }
    try await CatalogBroker(configuration: configuration).run()
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
    let body = CatalogRequestEnvelope.encode(tenant: "", payload: try encoder.encode(request))
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
    let body = CatalogRequestEnvelope.encode(tenant: "", payload: try encoder.encode(request))
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
