import Foundation

/// CatalogConvergenceInbox retains exact causal identity until its catalog delta is observed.
public actor CatalogConvergenceInbox {
  public enum InboxError: Error, Equatable, Sendable {
    case wrongTenant
    case wrongDomain
    case wrongGeneration
    case invalidRevision
    case invalidAffectedKeys
    case invalidTargets
    case conflictingNotification
    case notificationStreamFailed(String)
  }

  private let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private var pending: CatalogConvergenceNotification?
  private var acknowledgedRevision: UInt64 = 0
  private var observedCatalogRevisions: [String: UInt64] = [:]
  private var streamFailure: InboxError?

  public init(binding: CatalogFileProviderBinding, client: CatalogClient) {
    self.binding = binding
    self.client = client
  }

  public func receive(_ notification: CatalogConvergenceNotification) async throws {
    try validateBinding(notification)
    try Self.validatePayload(notification)
    if notification.revision <= acknowledgedRevision {
      return
    }
    guard try shouldAccept(notification) else { return }
    pending = notification
    try await acknowledgeIfObserved()
  }

  private func validateBinding(_ notification: CatalogConvergenceNotification) throws {
    guard notification.tenantID == binding.tenant.identifier else { throw InboxError.wrongTenant }
    guard notification.domainID == binding.domainID else { throw InboxError.wrongDomain }
    guard notification.generation == binding.tenant.generation else {
      throw InboxError.wrongGeneration
    }
  }

  private static func validatePayload(_ notification: CatalogConvergenceNotification) throws {
    guard notification.sourceRevision > 0, notification.catalogRevision > 0 else {
      throw InboxError.invalidRevision
    }
    guard !notification.affectedKeys.isEmpty,
          notification.affectedKeys == Array(Set(notification.affectedKeys)).sorted()
    else {
      throw InboxError.invalidAffectedKeys
    }
    let targetKeys = notification.targets.map(Self.targetKey)
    guard !targetKeys.isEmpty,
          notification.targets.allSatisfy(Self.validTarget),
          targetKeys.count == Set(targetKeys).count,
          targetKeys == targetKeys.sorted()
    else { throw InboxError.invalidTargets }
  }

  private func shouldAccept(_ notification: CatalogConvergenceNotification) throws -> Bool {
    guard let pending else { return true }
    guard notification.revision >= pending.revision else {
      throw InboxError.invalidRevision
    }
    if notification.revision == pending.revision {
      guard Self.same(notification, pending) else { throw InboxError.conflictingNotification }
      return false
    }
    guard notification.catalogRevision >= pending.catalogRevision,
          notification.sourceRevision >= pending.sourceRevision
    else {
      throw InboxError.invalidRevision
    }
    return true
  }

  public func acknowledgeObserved(
    target: CatalogSignalTarget,
    upTo revision: UInt64
  ) async throws {
    if let streamFailure {
      throw streamFailure
    }
    guard Self.validTarget(target) else { throw InboxError.invalidTargets }
    let key = Self.targetKey(target)
    observedCatalogRevisions[key] = max(observedCatalogRevisions[key, default: 0], revision)
    try await acknowledgeIfObserved()
  }

  private func acknowledgeIfObserved() async throws {
    guard let pending,
          pending.targets.allSatisfy({
            observedCatalogRevisions[Self.targetKey($0), default: 0] >= pending.catalogRevision
          })
    else { return }
    _ = try await client.acknowledge(tenant: binding.tenant, notification: pending)
    acknowledgedRevision = pending.revision
    self.pending = nil
  }

  public func fail(_ error: Error) {
    streamFailure = .notificationStreamFailed(String(describing: error))
  }

  public func checkHealthy() throws {
    if let streamFailure {
      throw streamFailure
    }
  }

  static func validTarget(_ target: CatalogSignalTarget) -> Bool {
    switch target.kind {
    case .workingSet:
      target.parentID == nil
    case .container:
      target.parentID != nil
    }
  }

  static func targetKey(_ target: CatalogSignalTarget) -> String {
    switch target.kind {
    case .workingSet:
      "working_set"
    case .container:
      "container:\(target.parentID?.rawValue ?? "")"
    }
  }

  static func same(
    _ lhs: CatalogConvergenceNotification,
    _ rhs: CatalogConvergenceNotification
  ) -> Bool {
    lhs.tenantID == rhs.tenantID
      && lhs.domainID == rhs.domainID
      && lhs.generation == rhs.generation
      && lhs.revision == rhs.revision
      && lhs.catalogRevision == rhs.catalogRevision
      && lhs.sourceRevision == rhs.sourceRevision
      && lhs.changeID == rhs.changeID
      && lhs.operationID == rhs.operationID
      && lhs.cause.rawValue == rhs.cause.rawValue
      && lhs.affectedKeys == rhs.affectedKeys
      && lhs.targets.count == rhs.targets.count
      && zip(lhs.targets, rhs.targets).allSatisfy {
        $0.kind.rawValue == $1.kind.rawValue && $0.parentID == $1.parentID
      }
  }
}
