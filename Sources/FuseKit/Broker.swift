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
    public let expectedRuntimeBuild: String
    public let noProgressTimeout: TimeInterval
    public let client: SocketClient.Configuration

    public init(
      appGroupEndpoint: CatalogAppGroupEndpoint,
      daemonSocketPath: String,
      expectedRuntimeBuild: String,
      noProgressTimeout: TimeInterval,
      client: SocketClient.Configuration = .init()
    ) {
      self.appGroupEndpoint = appGroupEndpoint
      self.daemonSocketPath = daemonSocketPath
      self.expectedRuntimeBuild = expectedRuntimeBuild
      self.noProgressTimeout = noProgressTimeout
      self.client = client
    }
  }

  private let daemon: SocketClient
  private let bridge: BrokerSocketBridge
  private let state: CatalogBrokerState

  public init(configuration: Configuration) async throws {
    daemon = try await SocketClient(
      path: configuration.daemonSocketPath,
      wireBuild: FuseKitTransportProtocol.wireBuild,
      role: FuseKitSessionPeerRole.broker,
      configuration: configuration.client
    )
    state = CatalogBrokerState(
      daemon: daemon,
      controller: CatalogDomainController()
    )
    bridge = try BrokerSocketBridge(
      container: configuration.appGroupEndpoint.container,
      socket: configuration.appGroupEndpoint.leaf,
      daemon: RuntimeClientConfiguration(
        path: configuration.daemonSocketPath,
        wireBuild: FuseKitTransportProtocol.wireBuild,
        role: FuseKitSessionPeerRole.broker,
        noProgressTimeout: configuration.noProgressTimeout,
        socket: configuration.client
      ),
      expectedRuntimeBuild: configuration.expectedRuntimeBuild
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
    let payload = try encoder.encode(CatalogBrokerOpenRequest())
    let call = try await daemon.open(
      operation: CatalogOperation.brokerOpen.rawValue,
      payload: payload,
      endInput: false
    )
    do {
      for try await chunk in call.chunks where !chunk.end {
        let command = try decoder.decode(CatalogBrokerCommand.self, from: chunk.payload)
        let result = await controller.execute(command)
        try await call.sendChunk(encoder.encode(result))
      }
      try await call.closeSend()
      let terminal = try await call.response()
      let response: CatalogBrokerOpenResponse = try Self.decodeTerminal(
        terminal,
        as: CatalogBrokerOpenResponse.self,
        decoder: decoder
      )
      guard response.code == .ok else {
        throw CatalogTransportError.remote(response.message)
      }
    } catch {
      await call.cancel()
      throw error
    }
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
