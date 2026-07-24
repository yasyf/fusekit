import Foundation

/// CatalogActivationInbox retains exact causal identity until its catalog delta is observed.
public actor CatalogActivationInbox {
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
  private var pending: CatalogActivationNotification?
  private var acknowledgedRevision: UInt64 = 0
  private var observedCatalogRevisions: [String: UInt64] = [:]
  private var observedTargetOrder: [String] = []
  private var streamFailure: InboxError?

  public init(binding: CatalogFileProviderBinding, client: CatalogClient) {
    self.binding = binding
    self.client = client
  }

  public func receive(_ notification: CatalogActivationNotification) async throws {
    try validateBinding(notification)
    try Self.validatePayload(notification)
    if notification.activationRevision <= acknowledgedRevision {
      return
    }
    guard try shouldAccept(notification) else { return }
    pending = notification
    retainObservedTargets(notification.targets)
    try await acknowledgeIfObserved()
  }

  private func validateBinding(_ notification: CatalogActivationNotification) throws {
    guard notification.tenantID == binding.tenant.identifier else { throw InboxError.wrongTenant }
    guard notification.domainID == binding.domainID else { throw InboxError.wrongDomain }
    guard notification.generation == binding.tenant.generation else {
      throw InboxError.wrongGeneration
    }
  }

  static func validatePayload(_ notification: CatalogActivationNotification) throws {
    guard notification.activationRevision > 0, notification.catalogHead > 0 else {
      throw InboxError.invalidRevision
    }
    guard Self.validDigest(notification.headDigest),
          Self.validDigest(notification.providerFingerprint),
          !notification.causes.isEmpty
    else {
      throw InboxError.invalidCausalMetadata
    }
    for (index, cause) in notification.causes.enumerated() {
      guard cause.sourceRevision > 0, Self.validDigest(cause.affectedKeysDigest) else {
        throw InboxError.invalidAffectedSummary
      }
      if index > 0 {
        let previous = notification.causes[index - 1]
        guard cause.sourceRevision > previous.sourceRevision
          || (cause.sourceRevision == previous.sourceRevision
            && cause.publicationID.rawValue > previous.publicationID.rawValue)
        else { throw InboxError.invalidCausalMetadata }
      }
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

  private func shouldAccept(_ notification: CatalogActivationNotification) throws -> Bool {
    guard let pending else { return true }
    guard notification.activationRevision >= pending.activationRevision else {
      throw InboxError.invalidRevision
    }
    if notification.activationRevision == pending.activationRevision {
      guard Self.same(notification, pending) else { throw InboxError.conflictingNotification }
      return false
    }
    guard notification.catalogHead >= pending.catalogHead else {
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
            observedCatalogRevisions[Self.targetKey($0), default: 0] >= pending.catalogHead
          })
    else { return }
    _ = try await client.acknowledge(tenant: binding.tenant, notification: pending)
    acknowledgedRevision = pending.activationRevision
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
      && digest.utf8.allSatisfy { (48 ... 57).contains($0) || (97 ... 102).contains($0) }
  }

  static func same(
    _ lhs: CatalogActivationNotification,
    _ rhs: CatalogActivationNotification
  ) -> Bool {
    lhs.tenantID == rhs.tenantID
      && lhs.domainID == rhs.domainID
      && lhs.generation == rhs.generation
      && lhs.activationChangeID == rhs.activationChangeID
      && lhs.activationRevision == rhs.activationRevision
      && lhs.catalogHead == rhs.catalogHead
      && lhs.headDigest == rhs.headDigest
      && lhs.providerFingerprint == rhs.providerFingerprint
      && lhs.causes.count == rhs.causes.count
      && zip(lhs.causes, rhs.causes).allSatisfy {
        $0.publicationID == $1.publicationID
          && $0.changeID == $1.changeID
          && $0.sourceRevision == $1.sourceRevision
          && $0.operationID == $1.operationID
          && $0.cause.rawValue == $1.cause.rawValue
          && $0.affectedKeysDigest == $1.affectedKeysDigest
      }
      && lhs.targetCount == rhs.targetCount
      && lhs.targetDigest == rhs.targetDigest
      && lhs.targetsCoalesced == rhs.targetsCoalesced
      && lhs.targets.count == rhs.targets.count
      && zip(lhs.targets, rhs.targets).allSatisfy {
        $0.kind.rawValue == $1.kind.rawValue && $0.parentID == $1.parentID
      }
  }
}
