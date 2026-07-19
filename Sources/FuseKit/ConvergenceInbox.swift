import Foundation

/// CatalogConvergenceInbox retains exact causal identity until its catalog delta is observed.
public actor CatalogConvergenceInbox {
  public enum InboxError: Error, Equatable, Sendable {
    case wrongTenant
    case wrongDomain
    case wrongGeneration
    case invalidRevision
    case invalidCausalMetadata
    case invalidAffectedSummary
    case invalidTargets
    case conflictingNotification
    case notificationStreamFailed(String)
  }

  private let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private var pending: CatalogConvergenceNotification?
  private var acknowledgedRevision: UInt64 = 0
  private var observedCatalogRevisions: [String: UInt64] = [:]
  private var observedTargetOrder: [String] = []
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
    retainObservedTargets(notification.targets)
    try await acknowledgeIfObserved()
  }

  private func validateBinding(_ notification: CatalogConvergenceNotification) throws {
    guard notification.tenantID == binding.tenant.identifier else { throw InboxError.wrongTenant }
    guard notification.domainID == binding.domainID else { throw InboxError.wrongDomain }
    guard notification.generation == binding.tenant.generation else {
      throw InboxError.wrongGeneration
    }
  }

  static func validatePayload(_ notification: CatalogConvergenceNotification) throws {
    guard notification.sourceRevision > 0, notification.catalogRevision > 0 else {
      throw InboxError.invalidRevision
    }
    let requiresOrigin = notification.cause == .providerMutation || notification.cause == .onDemand
    guard (notification.originDomain != nil) == requiresOrigin,
          (notification.originGeneration > 0) == requiresOrigin,
          Self.validDigest(notification.fingerprint)
    else {
      throw InboxError.invalidCausalMetadata
    }
    guard notification.affectedCount > 0, Self.validDigest(notification.affectedDigest) else {
      throw InboxError.invalidAffectedSummary
    }
    let targetKeys = notification.targets.map(Self.targetKey)
    guard !targetKeys.isEmpty,
          targetKeys.count <= Int(CatalogProtocol.maxSignalTargets),
          notification.targets.allSatisfy(Self.validTarget),
          targetKeys.count == Set(targetKeys).count,
          targetKeys == targetKeys.sorted(),
          notification.targetCount > 0,
          Self.validDigest(notification.targetDigest)
    else { throw InboxError.invalidTargets }
    if notification.targetsCoalesced {
      guard notification.targetCount > UInt64(CatalogProtocol.maxSignalTargets),
            notification.targets.count == 1,
            notification.targets[0].kind == .workingSet
      else { throw InboxError.invalidTargets }
    } else {
      guard notification.targetCount == UInt64(notification.targets.count) else {
        throw InboxError.invalidTargets
      }
    }
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
    if observedCatalogRevisions[key] == nil {
      if observedTargetOrder.count >= Int(CatalogProtocol.maxSignalTargets),
         let evicted = observedTargetOrder.first {
        observedTargetOrder.removeFirst()
        observedCatalogRevisions.removeValue(forKey: evicted)
      }
      observedTargetOrder.append(key)
    }
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
    observedCatalogRevisions.removeAll(keepingCapacity: true)
    observedTargetOrder.removeAll(keepingCapacity: true)
  }

  public func fail(_ error: Error) {
    streamFailure = .notificationStreamFailed(String(describing: error))
  }

  public func checkHealthy() throws {
    if let streamFailure {
      throw streamFailure
    }
  }

  func observedTargetCount() -> Int {
    observedCatalogRevisions.count
  }

  private func retainObservedTargets(_ targets: [CatalogSignalTarget]) {
    let retained = Set(targets.map(Self.targetKey))
    observedCatalogRevisions = observedCatalogRevisions.filter { retained.contains($0.key) }
    observedTargetOrder.removeAll { !retained.contains($0) }
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

  private static func validDigest(_ digest: String) -> Bool {
    digest.utf8.count == 64
      && digest.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
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
      && lhs.originDomain == rhs.originDomain
      && lhs.originGeneration == rhs.originGeneration
      && lhs.fingerprint == rhs.fingerprint
      && lhs.affectedCount == rhs.affectedCount
      && lhs.affectedDigest == rhs.affectedDigest
      && lhs.targetCount == rhs.targetCount
      && lhs.targetDigest == rhs.targetDigest
      && lhs.targetsCoalesced == rhs.targetsCoalesced
      && lhs.targets.count == rhs.targets.count
      && zip(lhs.targets, rhs.targets).allSatisfy {
        $0.kind.rawValue == $1.kind.rawValue && $0.parentID == $1.parentID
      }
  }
}
