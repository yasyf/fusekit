@testable import FuseKit

func makeNotification(revision: UInt64) throws -> CatalogConvergenceNotification {
  try CatalogConvergenceNotification(
    tenantID: CatalogTenantID("tenant-1"),
    domainID: domainID(),
    generation: 7,
    revision: revision,
    catalogRevision: revision + 100,
    sourceAuthority: CatalogSourceAuthorityID("source-main"),
    sourceRevision: revision,
    changeID: CatalogChangeID("11111111111111111111111111111111"),
    operationID: CatalogOperationID("22222222222222222222222222222222"),
    cause: .daemonWrite,
    originGeneration: 0,
    fingerprint: String(repeating: "c", count: 64),
    affectedCount: 1,
    affectedDigest: String(repeating: "a", count: 64),
    targetCount: 2,
    targetDigest: String(repeating: "b", count: 64),
    targetsCoalesced: false,
    targets: [
      CatalogSignalTarget(
        kind: .container,
        parentID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      ),
      CatalogSignalTarget(kind: .workingSet),
    ]
  )
}

func domainID(
  owner: String = "owner-1",
  account: String = "account-1"
) throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID(owner),
    accountInstanceID: CatalogAccountInstanceID(account)
  )
}

func registerDomain(_ system: RecordingDomainSystem) async throws {
  let ownerID = try CatalogOwnerID("owner-1")
  let accountID = try CatalogAccountInstanceID("account-1")
  _ = try await system.register(
    CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: ownerID, accountInstanceID: accountID),
      ownerID: ownerID,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accessMode: .readWrite,
      accountInstanceID: accountID,
      displayName: "Account 1"
    )
  )
}

func domainRegistration() throws -> CatalogDomainRegistration {
  let owner = try CatalogOwnerID("owner-1")
  let account = try CatalogAccountInstanceID("account-1")
  return try CatalogDomainRegistration(
    domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: account),
    ownerID: owner,
    tenantID: CatalogTenantID("tenant-1"),
    generation: 7,
    rootID: rootID(),
    accessMode: .readWrite,
    accountInstanceID: account,
    displayName: "Account 1"
  )
}

func rootID() throws -> CatalogObjectID {
  try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}

actor PublicationRecorder {
  private var values: [CatalogConvergenceNotification] = []

  func record(_ notification: CatalogConvergenceNotification) {
    values.append(notification)
  }

  func revisions() -> [UInt64] {
    values.map(\.revision)
  }
}

enum DomainSystemTestError: Error, Equatable {
  case conflict
}

actor RecordingDomainSystem: CatalogDomainSystem {
  private var signals: [(CatalogDomainID, CatalogSignalTarget)] = []
  private var signalCalls = 0
  private var domains: [CatalogDomainID: CatalogRegisteredDomain] = [:]

  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain {
    if let existing = domains[registration.domainID] {
      guard CatalogDomainRegistrationPolicy.matches(existing, registration) else {
        throw DomainSystemTestError.conflict
      }
      return existing
    }
    let registered = try CatalogRegisteredDomain(
      domainID: registration.domainID,
      ownerID: registration.ownerID,
      tenantID: registration.tenantID,
      generation: registration.generation,
      rootID: registration.rootID,
      accessMode: registration.accessMode,
      accountInstanceID: registration.accountInstanceID,
      displayName: registration.displayName,
      publicPath: "/public/\(registration.domainID.rawValue)"
    )
    domains[registration.domainID] = registered
    return registered
  }

  func remove(_: CatalogDomainID) async throws -> Bool {
    true
  }

  func list(after: CatalogDomainID?, limit: Int) async throws -> [CatalogRegisteredDomain] {
    Array(
      domains.values
        .sorted { $0.domainID.rawValue < $1.domainID.rawValue }
        .filter {
          guard let after else { return true }
          return $0.domainID.rawValue > after.rawValue
        }
        .prefix(limit + 1)
    )
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    guard let domain = domains[binding.domainID],
      domain.tenantID == binding.tenantID,
      domain.generation == binding.generation
    else { throw DomainSystemTestError.conflict }
  }

  func signal(domainID: CatalogDomainID, targets: [CatalogSignalTarget]) async throws {
    signalCalls += 1
    signals.append(contentsOf: targets.map { (domainID, $0) })
  }

  func signalKeys() -> [String] {
    signals.map { domain, target in
      switch target.kind {
      case .workingSet:
        "\(domain.rawValue):working_set"
      case .container:
        "\(domain.rawValue):container:\(target.parentID?.rawValue ?? "")"
      }
    }
  }

  func signalCallCount() -> Int {
    signalCalls
  }

  func registrationCount() -> Int {
    domains.count
  }
}
