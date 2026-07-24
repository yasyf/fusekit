@testable import FuseKit

func makeNotification(revision: UInt64) throws -> CatalogActivationNotification {
  try testActivationNotification(
    tenantID: CatalogTenantID("tenant-1"), domainID: domainID(), generation: 7,
    activationRevision: revision, catalogHead: revision + 100, sourceRevision: revision,
    targetCount: 2,
    targets: [
      CatalogSignalTarget(
        kind: .container,
        parentID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      ),
      CatalogSignalTarget(kind: .workingSet),
    ]
  )
}

func testActivationNotification(
  tenantID: CatalogTenantID,
  domainID: CatalogDomainID,
  generation: UInt64,
  activationRevision: UInt64,
  catalogHead: UInt64,
  activationChangeID: String = "33333333333333333333333333333333",
  sourceRevision: UInt64 = 1,
  changeID: String = "11111111111111111111111111111111",
  operationID: String = "22222222222222222222222222222222",
  cause: CatalogActivationCause = .daemonWrite,
  affectedKeysDigest: String = String(repeating: "a", count: 64),
  headDigest: String = String(repeating: "d", count: 64),
  providerFingerprint: String = String(repeating: "c", count: 64),
  targetCount: UInt64,
  targetDigest: String = String(repeating: "b", count: 64),
  targetsCoalesced: Bool = false,
  targets: [CatalogSignalTarget]
) throws -> CatalogActivationNotification {
  try CatalogActivationNotification(
    activationChangeID: CatalogActivationChangeID(activationChangeID),
    tenantID: tenantID,
    domainID: domainID,
    generation: generation,
    activationRevision: activationRevision,
    catalogHead: catalogHead,
    headDigest: headDigest,
    providerFingerprint: providerFingerprint,
    causes: [
      CatalogActivationSourceCause(
        publicationID: CatalogOperationID(operationID),
        changeID: CatalogChangeID(changeID),
        sourceRevision: sourceRevision,
        operationID: CatalogOperationID(operationID),
        cause: cause,
        affectedKeysDigest: affectedKeysDigest
      ),
    ],
    targetCount: targetCount,
    targetDigest: targetDigest,
    targetsCoalesced: targetsCoalesced,
    targets: targets
  )
}

func domainID(
  owner: String = "owner-1",
  account: String = "account-1"
) throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID(owner),
    presentationInstanceID: CatalogPresentationInstanceID(account)
  )
}

func registerDomain(_ system: RecordingDomainSystem) async throws {
  let ownerID = try CatalogOwnerID("owner-1")
  let accountID = try CatalogPresentationInstanceID("account-1")
  _ = try await system.register(
    CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: ownerID, presentationInstanceID: accountID),
      ownerID: ownerID,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accessMode: .readWrite,
      presentationInstanceID: accountID,
      displayName: "Account 1"
    )
  )
}

func domainRegistration() throws -> CatalogDomainRegistration {
  let owner = try CatalogOwnerID("owner-1")
  let account = try CatalogPresentationInstanceID("account-1")
  return try CatalogDomainRegistration(
    domainID: CatalogDomainID.derived(ownerID: owner, presentationInstanceID: account),
    ownerID: owner,
    tenantID: CatalogTenantID("tenant-1"),
    generation: 7,
    rootID: rootID(),
    accessMode: .readWrite,
    presentationInstanceID: account,
    displayName: "Account 1"
  )
}

func rootID() throws -> CatalogObjectID {
  try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}

func criticalReadinessProof(
  readProofDigest: String? = nil
) throws -> CatalogCriticalReadinessProof {
  let policyDigest = String(repeating: "1", count: 64)
  let resolutionDigest = String(repeating: "2", count: 64)
  let domain = try domainID()
  let account = try CatalogPresentationInstanceID("account-1")
  let root = try rootID()
  let tenant = try CatalogTenantID("tenant-1")
  let authority = try CatalogSourceAuthorityID("authority-1")
  let publication = try CatalogOperationID("11111111111111111111111111111111")
  let firstObject = try CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
  let secondObject = try CatalogObjectID("11111111111111111111111111111111")
  let lease = CatalogFileProviderLeaseReceipt(
    leaseID: "lease-1",
    tenantID: tenant,
    domainID: domain,
    generation: 7,
    rootID: root,
    presentationInstanceID: account,
    state: .provisional,
    sessionID: "session-1",
    processIdentity: "process-1",
    policyDigest: policyDigest,
    resolutionDigest: resolutionDigest,
    catalogHead: 12,
    sourceAuthority: authority,
    sourcePublication: publication,
    sourceRevision: 11,
    activationGeneration: "activation-1",
    expiresUnixNano: UInt64.max / 2
  )
  return try CatalogCriticalReadinessProof(
    policyDigest: policyDigest,
    resolutionDigest: resolutionDigest,
    catalogHead: 12,
    sourceRevision: 11,
    tenantGeneration: 7,
    domainID: domain,
    presentationInstanceID: account,
    rootID: root,
    activationGeneration: "activation-1",
    readChallenge: String(repeating: "5", count: 64),
    readProofDigest: readProofDigest,
    lease: lease,
    objects: [
      CatalogResolvedCriticalObjectProof(
        logicalID: "critical-a", role: "primary",
        objectID: firstObject,
        objectRevision: 12, contentRevision: 4, size: 5,
        hash: String(repeating: "3", count: 64)
      ),
      CatalogResolvedCriticalObjectProof(
        logicalID: "critical-b", role: "secondary",
        objectID: secondObject,
        objectRevision: 12, contentRevision: 5, size: 6,
        hash: String(repeating: "4", count: 64)
      ),
    ]
  )
}

enum DomainSystemTestError: Error, Equatable {
  case conflict
}

actor RecordingDomainSystem: CatalogDomainSystem {
  private var signals: [(CatalogDomainID, CatalogSignalTarget)] = []
  private var signalCalls = 0
  private var domains: [CatalogDomainID: CatalogRegisteredDomain] = [:]
  private var materializations: [CatalogCriticalReadinessProof] = []

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
      presentationInstanceID: registration.presentationInstanceID,
      displayName: registration.displayName,
      publicPath: "/public/\(registration.domainID.rawValue)"
    )
    domains[registration.domainID] = registered
    return registered
  }

  func remove(_ observedID: CatalogObservedDomainID) async throws -> Bool {
    if let identifier = try? observedID.decodedIdentifier(),
       let domainID = try? CatalogDomainID(identifier) {
      domains.removeValue(forKey: domainID)
    }
    return true
  }

  func list(
    after: CatalogObservedDomainID?, limit: Int
  ) async throws -> [CatalogObservedDomain] {
    try domains.values
      .map {
        try CatalogObservedDomain(
          observedID: CatalogObservedDomainID(observing: $0.domainID.rawValue), managed: $0
        )
      }
      .sorted { $0.observedID < $1.observedID }
      .filter {
        guard let after else { return true }
        return $0.observedID > after
      }
      .prefix(limit + 1)
      .map(\.self)
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

  func materializeCritical(
    _ readiness: CatalogCriticalReadinessProof
  ) async throws -> [CatalogCriticalMaterializationPath] {
    materializations.append(readiness)
    return try readiness.objects
      .sorted { $0.objectID.rawValue < $1.objectID.rawValue }
      .map {
        try CatalogCriticalMaterializationPath(
          objectID: $0.objectID,
          path: "/public/\(readiness.domainID.rawValue)/\($0.objectID.rawValue)"
        )
      }
  }

  func materializationCount() -> Int {
    materializations.count
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
