import DaemonKit
import Foundation

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

  func forwarding(operation: CatalogOperation, payload: Data) throws -> CatalogBrokerForwardRequest {
    try CatalogBrokerForwardRequest(
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
    -> CatalogSessionBinding {
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
    for id in Array(entries.keys) where entries[id]?.binding == route {
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
