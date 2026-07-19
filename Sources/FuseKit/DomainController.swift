@preconcurrency import FileProvider
import Foundation

protocol CatalogDomainSystem: Sendable {
  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain
  func remove(_ domainID: CatalogDomainID) async throws -> Bool
  func list() async throws -> [CatalogRegisteredDomain]
  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws
  func signal(domainID: CatalogDomainID, target: CatalogSignalTarget) async throws
}

enum CatalogDomainRegistrationPolicy {
  static func matches(
    _ existing: CatalogRegisteredDomain,
    _ registration: CatalogDomainRegistration
  ) -> Bool {
    existing.domainID == registration.domainID
      && existing.ownerID == registration.ownerID
      && existing.tenantID == registration.tenantID
      && existing.generation == registration.generation
      && existing.accountInstanceID == registration.accountInstanceID
      && existing.displayName == registration.displayName
  }
}

/// CatalogDomainController is the sole owner of File Provider domain lifecycle and signaling.
public actor CatalogDomainController {
  public enum ControllerError: Error, Equatable, Sendable {
    case invalidCommand
    case invalidBinding
    case staleNotification
    case conflictingNotification
  }

  private struct SignalProgress {
    let notification: CatalogConvergenceNotification
    var eventPublished: Bool
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
      switch command.kind {
      case .registerDomain:
        guard let registration = command.registration,
              command.domainID == nil,
              command.notification == nil
        else { throw ControllerError.invalidCommand }
        let registered = try await system.register(registration)
        if signals[registered.domainID]?.notification.generation != registered.generation {
          signals.removeValue(forKey: registered.domainID)
        }
        return CatalogBrokerResult(
          code: .ok,
          message: "",
          commandID: command.commandID,
          kind: command.kind,
          registered: registered
        )
      case .removeDomain:
        guard let domainID = command.domainID,
              command.registration == nil,
              command.notification == nil
        else { throw ControllerError.invalidCommand }
        await retire(domainID)
        let absent = try await system.remove(domainID)
        guard absent else { throw ControllerError.invalidCommand }
        signals.removeValue(forKey: domainID)
        return CatalogBrokerResult(
          code: .ok,
          message: "",
          commandID: command.commandID,
          kind: command.kind,
          confirmedAbsent: absent
        )
      case .listDomains:
        guard command.registration == nil,
              command.domainID == nil,
              command.notification == nil
        else { throw ControllerError.invalidCommand }
        let domains = try await system.list().sorted {
          $0.domainID.rawValue < $1.domainID.rawValue
        }
        return CatalogBrokerResult(
          code: .ok,
          message: "",
          commandID: command.commandID,
          kind: command.kind,
          domains: domains
        )
      case .signalDomain:
        guard let notification = command.notification,
              command.registration == nil,
              command.domainID == nil
        else { throw ControllerError.invalidCommand }
        try await signal(notification, publish: publish)
        return CatalogBrokerResult(
          code: .ok,
          message: "",
          commandID: command.commandID,
          kind: command.kind,
          signalAccepted: true
        )
      }
    } catch let error as ControllerError {
      return failure(command, code: .invalidRequest, message: String(describing: error))
    } catch {
      return failure(command, code: .unavailable, message: String(describing: error))
    }
  }

  private func signal(
    _ notification: CatalogConvergenceNotification,
    publish: @escaping @Sendable (CatalogConvergenceNotification) async throws -> Void
  ) async throws {
    guard notification.generation > 0,
          notification.revision > 0,
          notification.catalogRevision > 0,
          notification.sourceRevision > 0,
          !notification.affectedKeys.isEmpty,
          notification.affectedKeys == Array(Set(notification.affectedKeys)).sorted()
    else {
      throw ControllerError.invalidCommand
    }
    try await system.validate(
      CatalogBrokerBindDomainRequest(
        domainID: notification.domainID,
        tenantID: notification.tenantID,
        generation: notification.generation
      )
    )
    let targets = try Self.validatedTargets(notification.targets)
    var progress: SignalProgress
    if let existing = signals[notification.domainID] {
      if notification.revision < existing.notification.revision {
        throw ControllerError.staleNotification
      }
      if notification.revision == existing.notification.revision {
        guard CatalogConvergenceInbox.same(notification, existing.notification) else {
          throw ControllerError.conflictingNotification
        }
        progress = existing
      } else {
        guard notification.catalogRevision >= existing.notification.catalogRevision,
              notification.sourceRevision >= existing.notification.sourceRevision
        else {
          throw ControllerError.staleNotification
        }
        progress = SignalProgress(
          notification: notification,
          eventPublished: false,
          completedTargets: []
        )
      }
    } else {
      progress = SignalProgress(
        notification: notification,
        eventPublished: false,
        completedTargets: []
      )
    }
    if !progress.eventPublished {
      try await publish(notification)
      progress.eventPublished = true
      signals[notification.domainID] = progress
    }
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
    -> [CatalogSignalTarget]
  {
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
}

private final class FileProviderDomainSystem: CatalogDomainSystem, @unchecked Sendable {
  private struct RegistrationMetadata {
    let domainID: CatalogDomainID
    let ownerID: CatalogOwnerID
    let tenantID: CatalogTenantID
    let generation: UInt64
    let accountInstanceID: CatalogAccountInstanceID
  }

  private enum SystemError: Error {
    case conflictingRegistration
    case domainNotFound
    case invalidTarget
    case registrationMetadataMissing
    case registrationMismatch
  }

  private enum UserInfoKey {
    static let tenantID = "fusekit.tenant_id"
    static let ownerID = "fusekit.owner_id"
    static let generation = "fusekit.generation"
    static let accountInstanceID = "fusekit.account_instance_id"
  }

  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain {
    guard registration.generation > 0 else { throw SystemError.conflictingRegistration }
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == registration.domainID.rawValue
    }
    guard matches.count <= 1 else { throw SystemError.conflictingRegistration }
    if let existing = matches.first {
      let metadata = try metadata(existing)
      guard metadata.domainID == registration.domainID,
            metadata.ownerID == registration.ownerID,
            metadata.tenantID == registration.tenantID,
            metadata.generation == registration.generation,
            metadata.accountInstanceID == registration.accountInstanceID,
            existing.displayName == registration.displayName
      else {
        throw SystemError.conflictingRegistration
      }
      return try await registered(existing)
    }
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    domain.userInfo = [
      UserInfoKey.tenantID: registration.tenantID.rawValue,
      UserInfoKey.ownerID: registration.ownerID.rawValue,
      UserInfoKey.generation: String(registration.generation),
      UserInfoKey.accountInstanceID: registration.accountInstanceID.rawValue,
    ]
    try await NSFileProviderManager.add(domain)
    return try await registered(domain)
  }

  func remove(_ domainID: CatalogDomainID) async throws -> Bool {
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == domainID.rawValue
    }
    for domain in matches {
      try await NSFileProviderManager.remove(domain)
    }
    return try await !NSFileProviderManager.domains().contains {
      $0.identifier.rawValue == domainID.rawValue
    }
  }

  func list() async throws -> [CatalogRegisteredDomain] {
    var result: [CatalogRegisteredDomain] = []
    for domain in try await NSFileProviderManager.domains() {
      try await result.append(registered(domain))
    }
    return result
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == binding.domainID.rawValue
    }
    guard matches.count == 1, let domain = matches.first else {
      throw SystemError.domainNotFound
    }
    let metadata = try metadata(domain)
    guard metadata.domainID == binding.domainID,
          metadata.tenantID == binding.tenantID,
          metadata.generation == binding.generation
    else {
      throw SystemError.registrationMismatch
    }
  }

  func signal(domainID: CatalogDomainID, target: CatalogSignalTarget) async throws {
    guard
      let domain = try await NSFileProviderManager.domains().first(where: {
        $0.identifier.rawValue == domainID.rawValue
      }), let manager = NSFileProviderManager(for: domain)
    else {
      throw SystemError.domainNotFound
    }
    let identifier: NSFileProviderItemIdentifier
    switch target.kind {
    case .workingSet:
      identifier = .workingSet
    case .container:
      guard let parentID = target.parentID else { throw SystemError.invalidTarget }
      identifier = NSFileProviderItemIdentifier(parentID.rawValue)
    }
    try await manager.signalEnumerator(for: identifier)
  }

  private func registered(_ domain: NSFileProviderDomain) async throws -> CatalogRegisteredDomain {
    let metadata = try metadata(domain)
    guard let manager = NSFileProviderManager(for: domain) else {
      throw SystemError.registrationMetadataMissing
    }
    let url = try await manager.getUserVisibleURL(for: .rootContainer)
    return try CatalogRegisteredDomain(
      domainID: metadata.domainID,
      ownerID: metadata.ownerID,
      tenantID: metadata.tenantID,
      generation: metadata.generation,
      accountInstanceID: metadata.accountInstanceID,
      displayName: domain.displayName,
      publicPath: url.path
    )
  }

  private func metadata(_ domain: NSFileProviderDomain) throws -> RegistrationMetadata {
    guard let owner = domain.userInfo?[UserInfoKey.ownerID] as? String,
          let tenant = domain.userInfo?[UserInfoKey.tenantID] as? String,
          let generationText = domain.userInfo?[UserInfoKey.generation] as? String,
          let generation = UInt64(generationText), generation > 0,
          let account = domain.userInfo?[UserInfoKey.accountInstanceID] as? String
    else {
      throw SystemError.registrationMetadataMissing
    }
    let ownerID = try CatalogOwnerID(owner)
    let accountInstanceID = try CatalogAccountInstanceID(account)
    let domainID = try CatalogDomainID(domain.identifier.rawValue)
    guard
      domainID
      == CatalogDomainID.derived(
        ownerID: ownerID,
        accountInstanceID: accountInstanceID
      )
    else {
      throw SystemError.registrationMismatch
    }
    return try RegistrationMetadata(
      domainID: domainID,
      ownerID: ownerID,
      tenantID: CatalogTenantID(tenant),
      generation: generation,
      accountInstanceID: accountInstanceID
    )
  }
}
