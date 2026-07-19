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
      build: CatalogProtocol.daemonkitBuild,
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
      build: CatalogProtocol.daemonkitBuild,
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

struct CatalogSessionBinding: Hashable, Sendable {
  let domainID: CatalogDomainID
  let tenantID: CatalogTenantID
  let generation: UInt64

  init(_ request: CatalogBrokerBindDomainRequest) {
    domainID = request.domainID
    tenantID = request.tenantID
    generation = request.generation
  }

  init(_ notification: CatalogConvergenceNotification) {
    domainID = notification.domainID
    tenantID = notification.tenantID
    generation = notification.generation
  }

  func forwarding(operation: CatalogOperation, payload: Data) -> CatalogBrokerForwardRequest {
    CatalogBrokerForwardRequest(
      context: CatalogBrokerForwardContext(
        domainID: domainID,
        tenantID: tenantID,
        generation: generation
      ),
      operation: operation,
      payload: payload
    )
  }
}

enum CatalogSessionError: Error, Equatable, Sendable {
  case bindingTenantHeader
  case rebind
  case capacity
  case disconnected
  case revoked
  case unbound
  case wrongTenant
}

enum CatalogSessionBindingPolicy {
  static func accept(
    existing: CatalogSessionBinding?,
    candidate _: CatalogSessionBinding
  ) throws {
    guard existing == nil else { throw CatalogSessionError.rebind }
  }
}

protocol CatalogEventSession: AnyObject, Sendable {
  var isConnected: Bool { get }
  func waitUntilClosed() async
  func pushEvent(topic: String, payload: Data) async throws
}

extension SocketSession: CatalogEventSession {}

actor CatalogExtensionSessions {
  private struct Entry {
    let session: any CatalogEventSession
    let binding: CatalogSessionBinding
    var delivered: [CatalogDomainID: UInt64]
  }

  private let maximumSessions: Int
  private var entries: [ObjectIdentifier: Entry] = [:]
  private var revoked: [ObjectIdentifier: any CatalogEventSession] = [:]
  private var latest: [CatalogSessionBinding: CatalogConvergenceNotification] = [:]
  private let encoder: JSONEncoder

  init(maximumSessions: Int = 64) {
    precondition(maximumSessions > 0)
    self.maximumSessions = maximumSessions
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  func bind(_ session: any CatalogEventSession, to binding: CatalogSessionBinding) async throws {
    purgeDisconnected()
    let id = ObjectIdentifier(session)
    guard revoked[id] == nil else { throw CatalogSessionError.revoked }
    try CatalogSessionBindingPolicy.accept(existing: entries[id]?.binding, candidate: binding)
    guard session.isConnected else { throw CatalogSessionError.disconnected }
    guard entries.count + revoked.count < maximumSessions else {
      throw CatalogSessionError.capacity
    }
    entries[id] = Entry(session: session, binding: binding, delivered: [:])
    Task { [session] in
      await session.waitUntilClosed()
      self.remove(session)
    }
    if let notification = latest[binding] {
      do {
        try await deliver(notification, to: id)
      } catch {
        revoke(id)
        throw error
      }
    }
  }

  func authorize(_ session: any CatalogEventSession, tenant: String) throws
    -> CatalogSessionBinding
  {
    purgeDisconnected()
    let id = ObjectIdentifier(session)
    guard revoked[id] == nil else { throw CatalogSessionError.revoked }
    guard let entry = entries[id] else { throw CatalogSessionError.unbound }
    guard entry.binding.tenantID.rawValue == tenant else { throw CatalogSessionError.wrongTenant }
    return entry.binding
  }

  func publish(_ notification: CatalogConvergenceNotification) async throws {
    purgeDisconnected()
    let route = CatalogSessionBinding(notification)
    if let current = latest[route], current.revision > notification.revision {
      return
    }
    latest[route] = notification
    for id in Array(entries.keys) {
      guard entries[id]?.binding == route else { continue }
      do {
        try await deliver(notification, to: id)
      } catch {
        revoke(id)
      }
    }
  }

  private func deliver(
    _ notification: CatalogConvergenceNotification,
    to id: ObjectIdentifier
  ) async throws {
    guard let entry = entries[id] else { return }
    guard entry.delivered[notification.domainID, default: 0] < notification.revision else { return }
    try await entry.session.pushEvent(
      topic: CatalogOperation.convergenceNotify.rawValue,
      payload: encoder.encode(notification)
    )
    guard var current = entries[id], current.session === entry.session else { return }
    current.delivered[notification.domainID] = notification.revision
    entries[id] = current
  }

  func sessionCount() -> Int {
    purgeDisconnected()
    return entries.count
  }

  func retire(_ domainID: CatalogDomainID) {
    purgeDisconnected()
    latest = latest.filter { $0.key.domainID != domainID }
    let retiring = entries.filter { $0.value.binding.domainID == domainID }
    for (id, entry) in retiring {
      entries.removeValue(forKey: id)
      revoked[id] = entry.session
    }
  }

  func routeCount() -> Int {
    latest.count
  }

  func retainedSessionCount() -> Int {
    purgeDisconnected()
    return entries.count + revoked.count
  }

  private func remove(_ session: any CatalogEventSession) {
    let id = ObjectIdentifier(session)
    if entries[id]?.session === session {
      entries.removeValue(forKey: id)
    }
    if let current = revoked[id], current === session {
      revoked.removeValue(forKey: id)
    }
  }

  private func revoke(_ id: ObjectIdentifier) {
    guard let entry = entries.removeValue(forKey: id) else { return }
    revoked[id] = entry.session
  }

  private func purgeDisconnected() {
    entries = entries.filter(\.value.session.isConnected)
    revoked = revoked.filter(\.value.isConnected)
  }
}
