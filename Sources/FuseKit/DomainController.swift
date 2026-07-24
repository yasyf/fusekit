import Foundation

/// CatalogDomainController is the sole owner of File Provider domain lifecycle and signaling.
public actor CatalogDomainController {
  public enum ControllerError: Error, Equatable, Sendable {
    case invalidCommand
    case staleNotification
    case conflictingNotification
  }

  private struct SignalProgress {
    let notification: CatalogActivationNotification
    var completedTargets: Set<String>
  }

  private let system: any CatalogDomainSystem
  private var signals: [CatalogDomainID: SignalProgress] = [:]
  private var lastCommandID: UInt64 = 0

  public init() {
    system = FileProviderDomainSystem()
  }

  init(system: any CatalogDomainSystem) {
    self.system = system
  }

  public func execute(_ command: CatalogBrokerCommand) async -> CatalogBrokerResult {
    guard command.commandID > lastCommandID else {
      return failure(command, code: .invalidRequest, message: "command_id must increase")
    }
    lastCommandID = command.commandID
    do {
      return try await executeCommand(command)
    } catch let error as ControllerError {
      return failure(command, code: .invalidRequest, message: String(describing: error))
    } catch {
      return failure(command, code: .unavailable, message: String(describing: error))
    }
  }

  private func executeCommand(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    switch command.kind {
    case .registerDomain:
      try await register(command)
    case .removeDomain:
      try await remove(command)
    case .listDomains:
      try await list(command)
    case .signalDomain:
      try await signalCommand(command)
    case .materializeCritical:
      try await materializeCritical(command)
    }
  }

  private func register(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard let registration = command.registration,
          command.observedID == nil, command.notification == nil,
          command.afterObservedID == nil
    else { throw ControllerError.invalidCommand }
    let registered = try await system.register(registration)
    if signals[registered.domainID]?.notification.generation != registered.generation {
      signals.removeValue(forKey: registered.domainID)
    }
    return try CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, registered: registered
    )
  }

  private func remove(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard let observedID = command.observedID,
          command.registration == nil, command.notification == nil,
          command.afterObservedID == nil
    else { throw ControllerError.invalidCommand }
    if let identifier = try? observedID.decodedIdentifier(),
       let domainID = try? CatalogDomainID(identifier) {
      signals.removeValue(forKey: domainID)
    }
    let absent = try await system.remove(observedID)
    guard absent else { throw ControllerError.invalidCommand }
    return try CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, confirmedAbsent: absent
    )
  }

  private func list(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard command.registration == nil, command.observedID == nil,
          command.notification == nil
    else { throw ControllerError.invalidCommand }
    let limit = Int(CatalogProtocol.maxBrokerDomainPageSize)
    let window = try await system.list(after: command.afterObservedID, limit: limit)
    guard window.count <= limit + 1,
          window.map(\.observedID) == window.map(\.observedID).sorted(),
          Set(window.map(\.observedID)).count == window.count,
          window.allSatisfy({
            guard let after = command.afterObservedID else { return true }
            return $0.observedID > after
          })
    else { throw ControllerError.invalidCommand }
    let page = Array(window.prefix(limit))
    let next = window.count > limit ? page.last?.observedID : nil
    return try CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, domains: page, nextAfterObservedID: next
    )
  }

  private func signalCommand(_ command: CatalogBrokerCommand) async throws -> CatalogBrokerResult {
    guard let notification = command.notification,
          command.registration == nil, command.observedID == nil,
          command.afterObservedID == nil
    else { throw ControllerError.invalidCommand }
    try await signal(notification)
    return try CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, signalAccepted: true
    )
  }

  private func materializeCritical(
    _ command: CatalogBrokerCommand
  ) async throws -> CatalogBrokerResult {
    guard let readiness = command.criticalReadiness,
          command.registration == nil, command.observedID == nil,
          command.notification == nil, command.afterObservedID == nil,
          readiness.readProofDigest == nil
    else { throw ControllerError.invalidCommand }
    try Self.validateCriticalReadiness(readiness)
    let paths = try await system.materializeCritical(readiness)
    let expected = readiness.objects.map(\.objectID).sorted { $0.rawValue < $1.rawValue }
    guard paths.map(\.objectID) == expected,
          paths.allSatisfy(Self.validMaterializationPath)
    else { throw ControllerError.invalidCommand }
    return try CatalogBrokerResult(
      code: .ok, message: "", commandID: command.commandID,
      kind: command.kind, materializationScheduled: true,
      materializationPaths: paths
    )
  }

  private static func validateCriticalReadiness(
    _ readiness: CatalogCriticalReadinessProof
  ) throws {
    let lease = readiness.lease
    guard validDigest(readiness.policyDigest),
          validDigest(readiness.resolutionDigest),
          readiness.catalogHead != 0, readiness.sourceRevision != 0,
          readiness.tenantGeneration != 0,
          !readiness.activationGeneration.isEmpty,
          readiness.readProofDigest == nil,
          !readiness.objects.isEmpty, readiness.objects.count <= 32,
          lease.state == .provisional,
          lease.domainID == readiness.domainID,
          lease.generation == readiness.tenantGeneration,
          lease.rootID == readiness.rootID,
          lease.presentationInstanceID == readiness.presentationInstanceID,
          lease.policyDigest == readiness.policyDigest,
          lease.resolutionDigest == readiness.resolutionDigest,
          lease.catalogHead == readiness.catalogHead,
          lease.sourceRevision == readiness.sourceRevision,
          lease.activationGeneration == readiness.activationGeneration
    else { throw ControllerError.invalidCommand }
    let logicalIDs = readiness.objects.map(\.logicalID)
    guard logicalIDs == logicalIDs.sorted(), Set(logicalIDs).count == logicalIDs.count,
          Set(readiness.objects.map(\.objectID)).count == readiness.objects.count,
          readiness.objects.allSatisfy({
            !$0.logicalID.isEmpty && !$0.role.isEmpty
              && $0.objectRevision != 0 && $0.contentRevision != 0
              && validDigest($0.hash)
          })
    else { throw ControllerError.invalidCommand }
  }

  private static func validDigest(_ value: String) -> Bool {
    value.count == 64 && value.allSatisfy { "0123456789abcdef".contains($0) }
  }

  private static func validMaterializationPath(_ path: CatalogCriticalMaterializationPath) -> Bool {
    let value = path.path
    return !value.isEmpty && value.utf8.count <= Int(CatalogProtocol.maxPublicPathBytes)
      && value.hasPrefix("/") && !value.contains("\0")
      && URL(fileURLWithPath: value).standardizedFileURL.path == value
  }

  private func signal(_ notification: CatalogActivationNotification) async throws {
    try Self.validateNotification(notification)
    try await system.validate(
      CatalogBrokerBindDomainRequest(
        domainID: notification.domainID,
        tenantID: notification.tenantID,
        generation: notification.generation
      )
    )
    let targets = try Self.validatedTargets(notification.targets)
    let progress = try signalProgress(for: notification)
    try await signal(targets, notification: notification, progress: progress)
  }

  private static func validateNotification(_ notification: CatalogActivationNotification) throws {
    guard notification.generation > 0,
          notification.activationRevision > 0,
          notification.catalogHead > 0
    else {
      throw ControllerError.invalidCommand
    }
    do {
      try CatalogActivationInbox.validatePayload(notification)
    } catch {
      throw ControllerError.invalidCommand
    }
  }

  private func signalProgress(
    for notification: CatalogActivationNotification
  ) throws -> SignalProgress {
    guard let existing = signals[notification.domainID] else {
      return SignalProgress(notification: notification, completedTargets: [])
    }
    guard notification.activationRevision >= existing.notification.activationRevision else {
      throw ControllerError.staleNotification
    }
    if notification.activationRevision == existing.notification.activationRevision {
      guard CatalogActivationInbox.same(notification, existing.notification) else {
        throw ControllerError.conflictingNotification
      }
      return existing
    }
    guard notification.catalogHead >= existing.notification.catalogHead else {
      throw ControllerError.staleNotification
    }
    return SignalProgress(notification: notification, completedTargets: [])
  }

  private func signal(
    _ targets: [CatalogSignalTarget],
    notification: CatalogActivationNotification,
    progress initial: SignalProgress
  ) async throws {
    var progress = initial
    let pending = targets.filter { !progress.completedTargets.contains(Self.targetKey($0)) }
    guard !pending.isEmpty else { return }
    try await system.signal(domainID: notification.domainID, targets: pending)
    for target in pending {
      progress.completedTargets.insert(Self.targetKey(target))
    }
    signals[notification.domainID] = progress
  }

  private func failure(
    _ command: CatalogBrokerCommand,
    code: CatalogErrorCode,
    message: String
  ) -> CatalogBrokerResult {
    do {
      return try CatalogBrokerResult(
        code: code,
        message: Self.boundedMessage(message),
        commandID: command.commandID,
        kind: command.kind
      )
    } catch {
      preconditionFailure("FuseKit broker result construction failed: \(error)")
    }
  }

  private static func boundedMessage(_ message: String) -> String {
    if message.isEmpty {
      return "broker operation failed"
    }
    let limit = Int(CatalogProtocol.maxErrorMessageBytes)
    guard message.utf8.count > limit else { return message }
    var bounded = ""
    var size = 0
    for scalar in message.unicodeScalars {
      let scalarSize = String(scalar).utf8.count
      guard size + scalarSize <= limit else { break }
      bounded.unicodeScalars.append(scalar)
      size += scalarSize
    }
    return bounded
  }

  private static func validatedTargets(_ targets: [CatalogSignalTarget]) throws
    -> [CatalogSignalTarget] {
    guard !targets.isEmpty, targets.allSatisfy(CatalogActivationInbox.validTarget) else {
      throw ControllerError.invalidCommand
    }
    let keys = targets.map(targetKey)
    guard keys.count == Set(keys).count, keys == keys.sorted() else {
      throw ControllerError.invalidCommand
    }
    return targets
  }

  private static func targetKey(_ target: CatalogSignalTarget) -> String {
    CatalogActivationInbox.targetKey(target)
  }
}
