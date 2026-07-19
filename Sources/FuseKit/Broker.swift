import DaemonKit
import Foundation

/// CatalogBroker is the fixed host-app bridge between one App Group extension socket and one daemon session.
public final class CatalogBroker: @unchecked Sendable {
  /// Configuration pins the extension identity and the two fixed socket endpoints.
  public struct Configuration: Sendable {
    public let appGroupIdentifier: String
    public let appGroupSocketLeaf: String
    public let daemonSocketPath: String
    public let extensionTeamIdentifier: String
    public let extensionSigningIdentifier: String
    public let client: SocketClient.Configuration
    public let server: SocketServer.Configuration

    public init(
      appGroupIdentifier: String,
      appGroupSocketLeaf: String,
      daemonSocketPath: String,
      extensionTeamIdentifier: String,
      extensionSigningIdentifier: String,
      client: SocketClient.Configuration = .init(),
      server: SocketServer.Configuration = .init()
    ) {
      self.appGroupIdentifier = appGroupIdentifier
      self.appGroupSocketLeaf = appGroupSocketLeaf
      self.daemonSocketPath = daemonSocketPath
      self.extensionTeamIdentifier = extensionTeamIdentifier
      self.extensionSigningIdentifier = extensionSigningIdentifier
      self.client = client
      self.server = server
    }
  }

  private let server: SocketServer
  private let state: CatalogBrokerState

  public init(configuration: Configuration) throws {
    let daemon = try SocketClient(
      path: configuration.daemonSocketPath,
      build: FuseKitTransportProtocol.daemonkitBuild,
      configuration: configuration.client
    )
    let state = CatalogBrokerState(
      daemon: daemon,
      controller: CatalogDomainController()
    )
    let appGroup = try AppGroupContainer(identifier: configuration.appGroupIdentifier)
    let requirement = try PeerTrust.Requirement(
      teamIdentifier: configuration.extensionTeamIdentifier,
      signingIdentifier: configuration.extensionSigningIdentifier,
      requiredAppGroup: configuration.appGroupIdentifier
    )
    server = try SocketServer(
      path: appGroup.socketPath(leaf: configuration.appGroupSocketLeaf),
      build: FuseKitTransportProtocol.daemonkitBuild,
      configuration: configuration.server,
      trust: PeerTrust(requirement: requirement)
    ) { request in
      await state.forward(request)
    }
    self.state = state
  }

  /// run binds the App Group endpoint and owns the singleton broker.open stream until cancellation.
  public func run() async throws {
    try server.start()
    defer { server.stop() }
    try await state.runBroker()
  }
}

private actor CatalogBrokerState {
  private let daemon: SocketClient
  private let controller: CatalogDomainController
  private let sessions = CatalogExtensionSessions()
  private var activeCalls: [CatalogSessionBinding: [UUID: SocketCall]] = [:]
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
    let call = try daemon.open(
      operation: CatalogOperation.brokerOpen.rawValue,
      payload: payload,
      endInput: false
    )
    do {
      for try await chunk in call.chunks where !chunk.end {
        let command = try decoder.decode(CatalogBrokerCommand.self, from: chunk.payload)
        let result = await controller.execute(
          command,
          publish: { [sessions] notification in
            try await sessions.publish(notification)
          },
          retire: { domainID in
            await self.retire(domainID)
          }
        )
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
      call.cancel()
      throw error
    }
  }

  func forward(_ request: SocketRequest) async -> SocketResponse {
    guard let operation = CatalogOperation(rawValue: request.operation),
          operation != .brokerOpen,
          operation != .convergenceNotify,
          operation != .brokerForward
    else {
      return .terminal(SocketTerminal(rejected: true, reason: "unsupported FuseKit operation"))
    }
    do {
      if operation == .brokerBindDomain {
        return await bind(request)
      }
      let binding = try await sessions.authorize(request.session, tenant: request.tenant)
      let envelope = binding.forwarding(operation: operation, payload: request.payload)
      let call = try daemon.open(
        operation: CatalogOperation.brokerForward.rawValue,
        payload: encoder.encode(envelope),
        endInput: false
      )
      let callID = UUID()
      activeCalls[binding, default: [:]][callID] = call
      Task {
        do {
          for try await chunk in request.chunks {
            if chunk.end {
              try await call.closeSend()
              return
            }
            try await call.sendChunk(chunk.payload)
          }
          try await call.closeSend()
        } catch {
          call.cancel()
        }
      }
      return relay(call, binding: binding, callID: callID)
    } catch {
      return .terminal(SocketTerminal(error: String(describing: error)))
    }
  }

  private func relay(
    _ call: SocketCall,
    binding: CatalogSessionBinding,
    callID: UUID
  ) -> SocketResponse {
    .stream(
      SocketResponseStream(
        nextChunk: {
          while let chunk = try await call.chunks.nextChunk() {
            if !chunk.end {
              return chunk.payload
            }
          }
          return nil
        },
        terminal: {
          do {
            let terminal = try await call.response()
            await self.finished(binding: binding, callID: callID)
            return terminal
          } catch {
            await self.finished(binding: binding, callID: callID)
            throw error
          }
        },
        cancel: {
          call.cancel()
          Task { await self.finished(binding: binding, callID: callID) }
        }
      )
    )
  }

  private func retire(_ domainID: CatalogDomainID) async {
    await sessions.retire(domainID)
    let routes = activeCalls.keys.filter { $0.domainID == domainID }
    for route in routes {
      guard let calls = activeCalls.removeValue(forKey: route) else { continue }
      for call in calls.values {
        call.cancel()
      }
    }
  }

  private func finished(binding: CatalogSessionBinding, callID: UUID) {
    activeCalls[binding]?.removeValue(forKey: callID)
    if activeCalls[binding]?.isEmpty == true {
      activeCalls.removeValue(forKey: binding)
    }
  }

  private func bind(_ request: SocketRequest) async -> SocketResponse {
    do {
      guard request.tenant.isEmpty else {
        throw CatalogSessionError.bindingTenantHeader
      }
      let binding = try decoder.decode(CatalogBrokerBindDomainRequest.self, from: request.payload)
      try await controller.validate(binding)
      try await sessions.bind(request.session, to: CatalogSessionBinding(binding))
      return bindResponse(code: .ok, message: "")
    } catch CatalogSessionError.rebind {
      return bindResponse(code: .conflict, message: "session is already bound")
    } catch {
      return bindResponse(code: .invalidRequest, message: String(describing: error))
    }
  }

  private func bindResponse(code: CatalogErrorCode, message: String) -> SocketResponse {
    do {
      let response = CatalogBrokerBindDomainResponse(code: code, message: message)
      return try .terminal(SocketTerminal(payload: encoder.encode(response)))
    } catch {
      return .terminal(SocketTerminal(error: String(describing: error)))
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
