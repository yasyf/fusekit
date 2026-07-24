@preconcurrency import FileProvider
import Foundation

protocol CatalogDomainSystem: Sendable {
  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain
  func remove(_ observedID: CatalogObservedDomainID) async throws -> Bool
  func list(after: CatalogObservedDomainID?, limit: Int) async throws -> [CatalogObservedDomain]
  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws
  func signal(domainID: CatalogDomainID, targets: [CatalogSignalTarget]) async throws
  func materializeCritical(
    _ readiness: CatalogCriticalReadinessProof
  ) async throws -> [CatalogCriticalMaterializationPath]
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
      && existing.accessMode == registration.accessMode
      && existing.presentationInstanceID == registration.presentationInstanceID
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
    static let accessMode = "fusekit.access_mode"
    static let presentationInstanceID = "fusekit.presentation_instance_id"
  }

  let domainID: CatalogDomainID
  let ownerID: CatalogOwnerID
  let tenantID: CatalogTenantID
  let generation: UInt64
  let rootID: CatalogObjectID
  let accessMode: CatalogTenantAccessMode
  let presentationInstanceID: CatalogPresentationInstanceID

  init(registration: CatalogDomainRegistration) {
    domainID = registration.domainID
    ownerID = registration.ownerID
    tenantID = registration.tenantID
    generation = registration.generation
    rootID = registration.rootID
    accessMode = registration.accessMode
    presentationInstanceID = registration.presentationInstanceID
  }

  init(domain: NSFileProviderDomain) throws {
    guard let owner = domain.userInfo?[Key.ownerID] as? String,
          let tenant = domain.userInfo?[Key.tenantID] as? String,
          let generationText = domain.userInfo?[Key.generation] as? String,
          let generation = UInt64(generationText), generation > 0,
          let root = domain.userInfo?[Key.rootID] as? String,
          let access = domain.userInfo?[Key.accessMode] as? String,
          let accessMode = CatalogTenantAccessMode(rawValue: access),
          let account = domain.userInfo?[Key.presentationInstanceID] as? String
    else {
      throw CatalogDomainMetadataError.missing
    }
    let ownerID = try CatalogOwnerID(owner)
    let presentationInstanceID = try CatalogPresentationInstanceID(account)
    let domainID = try CatalogDomainID(domain.identifier.rawValue)
    guard
      domainID
      == CatalogDomainID.derived(
        ownerID: ownerID,
        presentationInstanceID: presentationInstanceID
      )
    else {
      throw CatalogDomainMetadataError.mismatch
    }
    self.domainID = domainID
    self.ownerID = ownerID
    tenantID = try CatalogTenantID(tenant)
    self.generation = generation
    rootID = try CatalogObjectID(root)
    self.accessMode = accessMode
    self.presentationInstanceID = presentationInstanceID
  }

  var userInfo: [String: String] {
    [
      Key.tenantID: tenantID.rawValue,
      Key.ownerID: ownerID.rawValue,
      Key.generation: String(generation),
      Key.rootID: rootID.rawValue,
      Key.accessMode: accessMode.rawValue,
      Key.presentationInstanceID: presentationInstanceID.rawValue,
    ]
  }

  static func declaresMetadata(_ domain: NSFileProviderDomain) -> Bool {
    guard let userInfo = domain.userInfo else { return false }
    return [
      Key.tenantID, Key.ownerID, Key.generation, Key.rootID, Key.accessMode,
      Key.presentationInstanceID,
    ]
    .contains { userInfo[$0] != nil }
  }
}
