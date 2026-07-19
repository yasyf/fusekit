@preconcurrency import FileProvider
import Foundation

protocol CatalogDomainSystem: Sendable {
  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain
  func remove(_ domainID: CatalogDomainID) async throws -> Bool
  func list() async throws -> [CatalogRegisteredDomain]
  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws
  func signal(domainID: CatalogDomainID, target: CatalogSignalTarget) async throws
  func cutover(_ plan: CatalogDomainCutoverPlan) async throws -> [CatalogDomainCutoverObservation]
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
      && existing.rootID == registration.rootID
      && existing.accountInstanceID == registration.accountInstanceID
      && existing.displayName == registration.displayName
  }
}

enum CatalogDomainMetadataError: Error, Equatable {
  case missing
  case mismatch
}

struct CatalogDomainMetadata: Equatable {
  private enum Key {
    static let tenantID = "fusekit.tenant_id"
    static let ownerID = "fusekit.owner_id"
    static let generation = "fusekit.generation"
    static let rootID = "fusekit.root_id"
    static let accountInstanceID = "fusekit.account_instance_id"
  }

  let domainID: CatalogDomainID
  let ownerID: CatalogOwnerID
  let tenantID: CatalogTenantID
  let generation: UInt64
  let rootID: CatalogObjectID
  let accountInstanceID: CatalogAccountInstanceID

  init(registration: CatalogDomainRegistration) {
    domainID = registration.domainID
    ownerID = registration.ownerID
    tenantID = registration.tenantID
    generation = registration.generation
    rootID = registration.rootID
    accountInstanceID = registration.accountInstanceID
  }

  init(domain: NSFileProviderDomain) throws {
    guard let owner = domain.userInfo?[Key.ownerID] as? String,
          let tenant = domain.userInfo?[Key.tenantID] as? String,
          let generationText = domain.userInfo?[Key.generation] as? String,
          let generation = UInt64(generationText), generation > 0,
          let root = domain.userInfo?[Key.rootID] as? String,
          let account = domain.userInfo?[Key.accountInstanceID] as? String
    else {
      throw CatalogDomainMetadataError.missing
    }
    let ownerID = try CatalogOwnerID(owner)
    let accountInstanceID = try CatalogAccountInstanceID(account)
    let domainID = try CatalogDomainID(domain.identifier.rawValue)
    guard domainID == CatalogDomainID.derived(
      ownerID: ownerID,
      accountInstanceID: accountInstanceID
    ) else {
      throw CatalogDomainMetadataError.mismatch
    }
    self.domainID = domainID
    self.ownerID = ownerID
    tenantID = try CatalogTenantID(tenant)
    self.generation = generation
    rootID = try CatalogObjectID(root)
    self.accountInstanceID = accountInstanceID
  }

  var userInfo: [String: String] {
    [
      Key.tenantID: tenantID.rawValue,
      Key.ownerID: ownerID.rawValue,
      Key.generation: String(generation),
      Key.rootID: rootID.rawValue,
      Key.accountInstanceID: accountInstanceID.rawValue,
    ]
  }

  static func declaresMetadata(_ domain: NSFileProviderDomain) -> Bool {
    guard let userInfo = domain.userInfo else { return false }
    return [Key.tenantID, Key.ownerID, Key.generation, Key.rootID, Key.accountInstanceID]
      .contains { userInfo[$0] != nil }
  }
}

enum CatalogDomainCutoverPolicy {
  static func observation(
    for domain: NSFileProviderDomain,
    plan: CatalogDomainCutoverPlan
  ) throws -> CatalogDomainCutoverObservation? {
    let domainID = domain.identifier.rawValue
    if let account = currentAccount(domainID: domainID, plan: plan) {
      return try currentObservation(domain: domain, account: account, plan: plan)
    }
    if let account = plan.accounts.first(where: { $0.legacyDomainID == domainID }) {
      guard !CatalogDomainMetadata.declaresMetadata(domain) else {
        throw CatalogDomainController.ControllerError.conflictingDomain
      }
      return observation(domainID: domainID, account: account, generation: 0, legacy: true)
    }
    return try metadataObservation(domain: domain, plan: plan)
  }

  private static func currentAccount(
    domainID: String,
    plan: CatalogDomainCutoverPlan
  ) -> CatalogDomainCutoverAccount? {
    plan.accounts.first {
      guard let instance = $0.accountInstanceID else { return false }
      return CatalogDomainID.derived(ownerID: plan.ownerID, accountInstanceID: instance).rawValue
        == domainID
    }
  }

  private static func currentObservation(
    domain: NSFileProviderDomain,
    account: CatalogDomainCutoverAccount,
    plan: CatalogDomainCutoverPlan
  ) throws -> CatalogDomainCutoverObservation {
    guard let instance = account.accountInstanceID else {
      throw CatalogDomainController.ControllerError.conflictingDomain
    }
    guard CatalogDomainMetadata.declaresMetadata(domain) else {
      return observation(
        domainID: domain.identifier.rawValue,
        account: account,
        generation: 0,
        accountInstanceID: instance
      )
    }
    let metadata = try CatalogDomainMetadata(domain: domain)
    guard metadata.ownerID == plan.ownerID, metadata.accountInstanceID == instance else {
      throw CatalogDomainController.ControllerError.conflictingDomain
    }
    return observation(
      domainID: domain.identifier.rawValue,
      account: account,
      generation: metadata.generation,
      accountInstanceID: instance
    )
  }

  private static func metadataObservation(
    domain: NSFileProviderDomain,
    plan: CatalogDomainCutoverPlan
  ) throws -> CatalogDomainCutoverObservation? {
    guard CatalogDomainMetadata.declaresMetadata(domain) else {
      throw CatalogDomainController.ControllerError.conflictingDomain
    }
    let metadata = try CatalogDomainMetadata(domain: domain)
    guard let account = plan.accounts.first(where: {
      $0.accountInstanceID == metadata.accountInstanceID
    }) else {
      guard metadata.ownerID != plan.ownerID else {
        throw CatalogDomainController.ControllerError.conflictingDomain
      }
      return nil
    }
    guard metadata.ownerID == plan.ownerID else {
      throw CatalogDomainController.ControllerError.conflictingDomain
    }
    return observation(
      domainID: domain.identifier.rawValue,
      account: account,
      generation: metadata.generation,
      accountInstanceID: metadata.accountInstanceID
    )
  }

  private static func observation(
    domainID: String,
    account: CatalogDomainCutoverAccount,
    generation: UInt64,
    accountInstanceID: CatalogAccountInstanceID? = nil,
    legacy: Bool = false
  ) -> CatalogDomainCutoverObservation {
    CatalogDomainCutoverObservation(
      domainID: domainID,
      accountID: account.accountID,
      immutableIdentity: account.immutableIdentity,
      generation: generation,
      accountInstanceID: accountInstanceID,
      legacy: legacy
    )
  }
}
