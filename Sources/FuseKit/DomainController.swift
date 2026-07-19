import Foundation

/// CatalogDomainController is the sole owner of File Provider domain lifecycle and signaling.
public actor CatalogDomainController {
  public enum ControllerError: Error, Equatable, Sendable {
    case invalidCommand
    case invalidBinding
    case staleNotification
    case conflictingNotification
    case conflictingDomain
  }

  private struct SignalProgress {
    let notification: CatalogConvergenceNotification
    var eventPublished: Bool
    var completedTargets: Set<String>
  }

  private let system: any CatalogDomainSystem
  private var signals: [CatalogDomainID: SignalProgress] = [:]
  private var lastCommandID: UInt64 = 0
  private let now: @Sendable () -> Date

  public init() {
    system = FileProviderDomainSystem()
    now = Date.init
  }

  init(system: any CatalogDomainSystem, now: @escaping @Sendable () -> Date = Date.init) {
    self.system = system
    self.now = now
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    guard binding.generation > 0 else { throw ControllerError.invalidBinding }
    try await system.validate(binding)
  }

  public func execute(
    _ command: CatalogBrokerCommand,
    publish: @escaping @Sendable (CatalogConvergenceNotification) async throws -> Void,
    retire: @escaping @Sendable (CatalogDomainID) async -> Void = { _ in }
  ) async -> CatalogBrokerResult {
    guard command.commandID > lastCommandID else {
      return failure(command, code: .invalidRequest, message: "command_id must increase")
    }
    lastCommandID = command.commandID
    do {
      return try await executeCommand(command, publish: publish, retire: retire)
    } catch let error as ControllerError {
      return failure(command, code: .invalidRequest, message: String(describing: error))
    } catch {
      return failure(command, code: .unavailable, message: String(describing: error))
    }
  }

  private func executeCommand(
    _ command: CatalogBrokerCommand,
    publish: @escaping @Sendable (CatalogConvergenceNotification) async throws -> Void,
    retire: @escaping @Sendable (CatalogDomainID) async -> Void
  ) async throws -> CatalogBrokerResult {
    switch command.kind {
    case .registerDomain:
      try await register(command)
    case .removeDomain:
      try await remove(command, retire: retire)
    case .listDomains:
      try await list(command)
    case .signalDomain:
      try await signalCommand(command, publish: publish)
    case .cutoverDomains:
      try await cutover(command)
    }
  }

  private func register(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard let registration = command.registration,
          command.domainID == nil, command.notification == nil, command.cutover == nil
    else { throw ControllerError.invalidCommand }
    let registered = try await system.register(registration)
    if signals[registered.domainID]?.notification.generation != registered.generation {
      signals.removeValue(forKey: registered.domainID)
    }
    return CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, registered: registered
    )
  }

  private func remove(
    _ command: CatalogBrokerCommand,
    retire: @escaping @Sendable (CatalogDomainID) async -> Void
  ) async throws -> CatalogBrokerResult {
    guard let domainID = command.domainID,
          command.registration == nil, command.notification == nil, command.cutover == nil
    else { throw ControllerError.invalidCommand }
    await retire(domainID)
    let absent = try await system.remove(domainID)
    guard absent else { throw ControllerError.invalidCommand }
    signals.removeValue(forKey: domainID)
    return CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, confirmedAbsent: absent
    )
  }

  private func list(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard command.registration == nil, command.domainID == nil,
          command.notification == nil, command.cutover == nil
    else { throw ControllerError.invalidCommand }
    let domains = try await system.list().sorted { $0.domainID.rawValue < $1.domainID.rawValue }
    return CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, domains: domains
    )
  }

  private func signalCommand(
    _ command: CatalogBrokerCommand,
    publish: @escaping @Sendable (CatalogConvergenceNotification) async throws -> Void
  ) async throws -> CatalogBrokerResult {
    guard let notification = command.notification,
          command.registration == nil, command.domainID == nil, command.cutover == nil
    else { throw ControllerError.invalidCommand }
    try await signal(notification, publish: publish)
    return CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, signalAccepted: true
    )
  }

  private func cutover(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard let plan = command.cutover,
          command.registration == nil, command.domainID == nil, command.notification == nil
    else { throw ControllerError.invalidCommand }
    try Self.validateCutoverPlan(plan)
    let observed = try await system.cutover(plan).sorted { $0.domainID < $1.domainID }
    return CatalogBrokerResult(
      code: .ok,
      message: "",
      commandID: command.commandID,
      kind: command.kind,
      cutoverResult: CatalogDomainCutoverResult(
        plan: plan,
        observedDomains: observed,
        finalEnumerationRevision: command.commandID,
        finalEnumeratedAtUnixNano: Int64(now().timeIntervalSince1970 * 1_000_000_000)
      )
    )
  }

  private func signal(
    _ notification: CatalogConvergenceNotification,
    publish: @escaping @Sendable (CatalogConvergenceNotification) async throws -> Void
  ) async throws {
    try Self.validateNotification(notification)
    try await system.validate(
      CatalogBrokerBindDomainRequest(
        domainID: notification.domainID,
        tenantID: notification.tenantID,
        generation: notification.generation
      )
    )
    let targets = try Self.validatedTargets(notification.targets)
    var progress = try signalProgress(for: notification)
    if !progress.eventPublished {
      try await publish(notification)
      progress.eventPublished = true
      signals[notification.domainID] = progress
    }
    try await signal(targets, notification: notification, progress: progress)
  }

  private static func validateNotification(_ notification: CatalogConvergenceNotification) throws {
    guard notification.generation > 0,
          notification.revision > 0,
          notification.catalogRevision > 0,
          notification.sourceRevision > 0,
          !notification.affectedKeys.isEmpty,
          notification.affectedKeys == Array(Set(notification.affectedKeys)).sorted()
    else {
      throw ControllerError.invalidCommand
    }
  }

  private func signalProgress(
    for notification: CatalogConvergenceNotification
  ) throws -> SignalProgress {
    guard let existing = signals[notification.domainID] else {
      return SignalProgress(notification: notification, eventPublished: false, completedTargets: [])
    }
    guard notification.revision >= existing.notification.revision else {
      throw ControllerError.staleNotification
    }
    if notification.revision == existing.notification.revision {
      guard CatalogConvergenceInbox.same(notification, existing.notification) else {
        throw ControllerError.conflictingNotification
      }
      return existing
    }
    guard notification.catalogRevision >= existing.notification.catalogRevision,
          notification.sourceRevision >= existing.notification.sourceRevision
    else {
      throw ControllerError.staleNotification
    }
    return SignalProgress(notification: notification, eventPublished: false, completedTargets: [])
  }

  private func signal(
    _ targets: [CatalogSignalTarget],
    notification: CatalogConvergenceNotification,
    progress initial: SignalProgress
  ) async throws {
    var progress = initial
    for target in targets {
      let key = Self.targetKey(target)
      guard !progress.completedTargets.contains(key) else { continue }
      try await system.signal(domainID: notification.domainID, target: target)
      progress.completedTargets.insert(key)
      signals[notification.domainID] = progress
    }
  }

  private func failure(
    _ command: CatalogBrokerCommand,
    code: CatalogErrorCode,
    message: String
  ) -> CatalogBrokerResult {
    CatalogBrokerResult(
      code: code,
      message: message,
      commandID: command.commandID,
      kind: command.kind
    )
  }

  private static func validatedTargets(_ targets: [CatalogSignalTarget]) throws
    -> [CatalogSignalTarget] {
    guard !targets.isEmpty, targets.allSatisfy(CatalogConvergenceInbox.validTarget) else {
      throw ControllerError.invalidCommand
    }
    let keys = targets.map(targetKey)
    guard keys.count == Set(keys).count, keys == keys.sorted() else {
      throw ControllerError.invalidCommand
    }
    return targets
  }

  private static func targetKey(_ target: CatalogSignalTarget) -> String {
    CatalogConvergenceInbox.targetKey(target)
  }

  private static func validateCutoverPlan(_ plan: CatalogDomainCutoverPlan) throws {
    guard !plan.accounts.isEmpty else { throw ControllerError.invalidCommand }
    var prior: UInt64 = 0
    var instances: Set<CatalogAccountInstanceID> = []
    for account in plan.accounts {
      guard account.accountID > prior,
            account.legacyDomainID == String(format: "acct-%02llu", account.accountID),
            account.immutableIdentity.count == 64,
            account.immutableIdentity.allSatisfy({ "0123456789abcdef".contains($0) })
      else { throw ControllerError.invalidCommand }
      prior = account.accountID
      if let instance = account.accountInstanceID {
        guard instances.insert(instance).inserted else { throw ControllerError.invalidCommand }
      }
    }
  }
}
