@preconcurrency import FileProvider
@testable import FuseKit
import Testing

@Suite("Domain signaling")
struct DomainControllerTests {
  @Test
  func cutoverProofBindsExactPlanAndFreshEnumeration() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(
      system: system,
      now: { Date(timeIntervalSince1970: 123) }
    )
    let plan = try CatalogDomainCutoverPlan(
      operationID: CatalogMutationID("33333333333333333333333333333333"),
      ownerID: CatalogOwnerID("owner-1"),
      accounts: [
        CatalogDomainCutoverAccount(
          accountID: 1,
          immutableIdentity: String(repeating: "a", count: 64),
          legacyDomainID: "acct-01"
        ),
      ]
    )
    await system.setCutoverObservations([
      CatalogDomainCutoverObservation(
        domainID: "acct-01",
        accountID: 1,
        immutableIdentity: String(repeating: "a", count: 64),
        generation: 0,
        legacy: true
      ),
    ])

    let first = try await controller.execute(
      CatalogBrokerCommand(commandID: 1, kind: .cutoverDomains, cutover: plan),
      publish: { _ in }
    )
    let replay = try await controller.execute(
      CatalogBrokerCommand(commandID: 2, kind: .cutoverDomains, cutover: plan),
      publish: { _ in }
    )

    #expect(first.code == .ok)
    #expect(first.cutoverResult?.plan.operationID == plan.operationID)
    #expect(first.cutoverResult?.observedDomains.map(\.domainID) == ["acct-01"])
    #expect(first.cutoverResult?.finalEnumerationRevision == 1)
    #expect(first.cutoverResult?.finalEnumeratedAtUnixNano == 123_000_000_000)
    #expect(replay.cutoverResult?.finalEnumerationRevision == 2)
    #expect(await system.cutoverCount() == 2)
  }

  @Test
  func cutoverRejectsAccountDomainMismatchBeforeSystemAccess() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let plan = try CatalogDomainCutoverPlan(
      operationID: CatalogMutationID("33333333333333333333333333333333"),
      ownerID: CatalogOwnerID("owner-1"),
      accounts: [
        CatalogDomainCutoverAccount(
          accountID: 1,
          immutableIdentity: String(repeating: "a", count: 64),
          legacyDomainID: "acct-02"
        ),
      ]
    )
    let result = try await controller.execute(
      CatalogBrokerCommand(commandID: 1, kind: .cutoverDomains, cutover: plan),
      publish: { _ in }
    )
    #expect(result.code == .invalidRequest)
    #expect(await system.cutoverCount() == 0)
  }

  @Test
  func cutoverPolicyAcceptsOnlyExactLegacyOrDerivedCurrentIdentity() throws {
    let owner = try CatalogOwnerID("owner-1")
    let instance = try CatalogAccountInstanceID("account-1")
    let plan = try CatalogDomainCutoverPlan(
      operationID: CatalogMutationID("33333333333333333333333333333333"),
      ownerID: owner,
      accounts: [
        CatalogDomainCutoverAccount(
          accountID: 1,
          immutableIdentity: String(repeating: "a", count: 64),
          legacyDomainID: "acct-01",
          accountInstanceID: instance
        ),
      ]
    )
    let legacy = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier("acct-01"),
      displayName: "acct-01"
    )
    #expect(try CatalogDomainCutoverPolicy.observation(for: legacy, plan: plan)?.legacy == true)

    let registration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: instance),
      ownerID: owner,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accountInstanceID: instance,
      displayName: "Account 1"
    )
    let current = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    current.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    #expect(try CatalogDomainCutoverPolicy.observation(for: current, plan: plan)?.generation == 7)

    let metadataFreeCurrent = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    let metadataFreeObservation = try CatalogDomainCutoverPolicy.observation(
      for: metadataFreeCurrent,
      plan: plan
    )
    #expect(metadataFreeObservation?.legacy == false)
    #expect(metadataFreeObservation?.generation == 0)
    #expect(metadataFreeObservation?.accountInstanceID == instance)

    let unknownMetadataFree = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier("opaque-unplanned-domain"),
      displayName: "Unknown"
    )
    #expect(throws: CatalogDomainController.ControllerError.conflictingDomain) {
      _ = try CatalogDomainCutoverPolicy.observation(for: unknownMetadataFree, plan: plan)
    }

    let unplannedInstance = try CatalogAccountInstanceID("account-2")
    let unplannedRegistration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: unplannedInstance),
      ownerID: owner,
      tenantID: CatalogTenantID("tenant-2"),
      generation: 1,
      rootID: rootID(),
      accountInstanceID: unplannedInstance,
      displayName: "Unplanned account"
    )
    let unplanned = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(unplannedRegistration.domainID.rawValue),
      displayName: unplannedRegistration.displayName
    )
    unplanned.userInfo = CatalogDomainMetadata(registration: unplannedRegistration).userInfo
    #expect(throws: CatalogDomainController.ControllerError.conflictingDomain) {
      _ = try CatalogDomainCutoverPolicy.observation(for: unplanned, plan: plan)
    }

    let foreignOwner = try CatalogOwnerID("owner-2")
    let foreignRegistration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: foreignOwner, accountInstanceID: unplannedInstance),
      ownerID: foreignOwner,
      tenantID: CatalogTenantID("tenant-foreign"),
      generation: 1,
      rootID: rootID(),
      accountInstanceID: unplannedInstance,
      displayName: "Foreign account"
    )
    let foreign = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(foreignRegistration.domainID.rawValue),
      displayName: foreignRegistration.displayName
    )
    foreign.userInfo = CatalogDomainMetadata(registration: foreignRegistration).userInfo
    #expect(try CatalogDomainCutoverPolicy.observation(for: foreign, plan: plan) == nil)

    let stray = try NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(
        CatalogDomainID.derived(
          ownerID: CatalogOwnerID("owner-2"), accountInstanceID: instance
        ).rawValue
      ),
      displayName: registration.displayName
    )
    stray.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    #expect(throws: CatalogDomainMetadataError.mismatch) {
      _ = try CatalogDomainCutoverPolicy.observation(for: stray, plan: plan)
    }
  }

  @Test
  func exactNotificationAndTargetsAreCoalescedOnce() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let publications = PublicationRecorder()
    try await registerDomain(system)
    let notification = try makeNotification(revision: 12)
    let first = try CatalogBrokerCommand(
      commandID: 1,
      kind: .signalDomain,
      notification: notification
    )
    let duplicate = try CatalogBrokerCommand(
      commandID: 2,
      kind: .signalDomain,
      notification: notification
    )

    let firstResult = await controller.execute(first) { value in
      await publications.record(value)
    }
    let duplicateResult = await controller.execute(duplicate) { value in
      await publications.record(value)
    }

    #expect(firstResult.code == .ok)
    #expect(duplicateResult.code == .ok)
    #expect(await publications.revisions() == [12])
    let domainID = notification.domainID.rawValue
    #expect(
      await system.signalKeys() == [
        "\(domainID):container:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "\(domainID):working_set",
      ]
    )
  }

  @Test
  func staleNotificationIsRejectedWithoutSignaling() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let publications = PublicationRecorder()
    try await registerDomain(system)
    for revision in [12, 11] {
      let notification = try makeNotification(revision: UInt64(revision))
      let command = try CatalogBrokerCommand(
        commandID: UInt64(revision),
        kind: .signalDomain,
        notification: notification
      )
      let result = await controller.execute(command) { value in
        await publications.record(value)
      }
      if revision == 11 {
        #expect(result.code == .invalidRequest)
      }
    }

    #expect(await publications.revisions() == [12])
    #expect(await system.signalKeys().count == 2)
  }

  @Test
  func registrationReplayIsIdempotentAndMetadataDriftConflicts() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let ownerID = try CatalogOwnerID("owner-1")
    let accountID = try CatalogAccountInstanceID("account-1")
    let registration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: ownerID, accountInstanceID: accountID),
      ownerID: ownerID,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accountInstanceID: accountID,
      displayName: "Account 1"
    )
    for commandID: UInt64 in [1, 2] {
      let result = try await controller.execute(
        CatalogBrokerCommand(
          commandID: commandID,
          kind: .registerDomain,
          registration: registration
        ),
        publish: { _ in }
      )
      #expect(result.code == .ok)
      #expect(result.registered?.generation == 7)
    }

    let conflict = try await controller.execute(
      CatalogBrokerCommand(
        commandID: 3,
        kind: .registerDomain,
        registration: CatalogDomainRegistration(
          domainID: registration.domainID,
          ownerID: registration.ownerID,
          tenantID: registration.tenantID,
          generation: registration.generation,
          rootID: CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
          accountInstanceID: registration.accountInstanceID,
          displayName: registration.displayName
        )
      ),
      publish: { _ in }
    )
    #expect(conflict.code == .unavailable)
    #expect(await system.registrationCount() == 1)
  }

  @Test
  func registeredMetadataRebuildsExactBindingSynchronously() throws {
    let registration = try domainRegistration()
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    domain.userInfo = CatalogDomainMetadata(registration: registration).userInfo

    let binding = try CatalogFileProviderBinding(domain: domain)

    #expect(binding.domainID == registration.domainID)
    #expect(binding.tenant.identifier == registration.tenantID)
    #expect(binding.tenant.generation == registration.generation)
    #expect(binding.rootID == registration.rootID)
  }

  @Test
  func registeredMetadataRejectsMissingBadAndMismatchedIdentity() throws {
    let registration = try domainRegistration()
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    #expect(throws: CatalogDomainMetadataError.missing) {
      _ = try CatalogFileProviderBinding(domain: domain)
    }
    var bad = CatalogDomainMetadata(registration: registration).userInfo
    let rootKey = try #require(bad.first(where: { $0.value == registration.rootID.rawValue })?.key)
    bad[rootKey] = "bad"
    domain.userInfo = bad
    #expect(throws: Error.self) {
      _ = try CatalogFileProviderBinding(domain: domain)
    }
    let mismatched = try NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(
        CatalogDomainID.derived(
          ownerID: CatalogOwnerID("owner-2"),
          accountInstanceID: CatalogAccountInstanceID("account-2")
        ).rawValue
      ),
      displayName: registration.displayName
    )
    mismatched.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    #expect(throws: CatalogDomainMetadataError.mismatch) {
      _ = try CatalogFileProviderBinding(domain: mismatched)
    }
  }

  @Test
  func bindingRejectsAnyDomainTenantOrGenerationChange() throws {
    let accepted = try CatalogSessionBinding(
      CatalogBrokerBindDomainRequest(
        domainID: domainID(),
        tenantID: CatalogTenantID("tenant-1"),
        generation: 7
      )
    )
    try CatalogSessionBindingPolicy.accept(existing: nil, candidate: accepted)
    #expect(throws: CatalogSessionError.rebind) {
      try CatalogSessionBindingPolicy.accept(existing: accepted, candidate: accepted)
    }

    let candidates = try [
      CatalogBrokerBindDomainRequest(
        domainID: domainID(owner: "owner-2", account: "account-2"),
        tenantID: accepted.tenantID,
        generation: accepted.generation
      ),
      CatalogBrokerBindDomainRequest(
        domainID: accepted.domainID,
        tenantID: CatalogTenantID("tenant-2"),
        generation: accepted.generation
      ),
      CatalogBrokerBindDomainRequest(
        domainID: accepted.domainID,
        tenantID: accepted.tenantID,
        generation: accepted.generation + 1
      ),
    ]
    for candidate in candidates {
      #expect(throws: CatalogSessionError.rebind) {
        try CatalogSessionBindingPolicy.accept(
          existing: accepted,
          candidate: CatalogSessionBinding(candidate)
        )
      }
    }
  }

  @Test
  func initialBindingMustMatchRegisteredDomainTenantAndGeneration() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let ownerID = try CatalogOwnerID("owner-1")
    let accountID = try CatalogAccountInstanceID("account-1")
    let registration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: ownerID, accountInstanceID: accountID),
      ownerID: ownerID,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accountInstanceID: accountID,
      displayName: "Account 1"
    )
    _ = try await system.register(registration)
    try await controller.validate(
      CatalogBrokerBindDomainRequest(
        domainID: registration.domainID,
        tenantID: registration.tenantID,
        generation: registration.generation
      )
    )

    let invalid = try [
      CatalogBrokerBindDomainRequest(
        domainID: domainID(owner: "owner-2", account: "account-2"),
        tenantID: registration.tenantID,
        generation: registration.generation
      ),
      CatalogBrokerBindDomainRequest(
        domainID: registration.domainID,
        tenantID: CatalogTenantID("tenant-2"),
        generation: registration.generation
      ),
      CatalogBrokerBindDomainRequest(
        domainID: registration.domainID,
        tenantID: registration.tenantID,
        generation: registration.generation + 1
      ),
    ]
    for binding in invalid {
      await #expect(throws: DomainSystemTestError.conflict) {
        try await controller.validate(binding)
      }
    }
  }

  @Test
  func commandIdentifiersMustStrictlyIncrease() async throws {
    let controller = CatalogDomainController(system: RecordingDomainSystem())
    let command = try CatalogBrokerCommand(commandID: 1, kind: .listDomains)
    let first = await controller.execute(command, publish: { _ in })
    let replay = await controller.execute(command, publish: { _ in })

    #expect(first.code == .ok)
    #expect(replay.code == .invalidRequest)
    #expect(replay.message.contains("increase"))
  }

  @Test
  func registeredDomainRejectsOwnerAccountIdentityMismatch() throws {
    let owner = try CatalogOwnerID("owner-1")
    let account = try CatalogAccountInstanceID("account-1")
    let domainID = CatalogDomainID.derived(ownerID: owner, accountInstanceID: account)

    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogRegisteredDomain(
        domainID: domainID,
        ownerID: CatalogOwnerID("owner-2"),
        tenantID: CatalogTenantID("tenant-1"),
        generation: 1,
        rootID: rootID(),
        accountInstanceID: account,
        displayName: "Account 1",
        publicPath: "/tmp/account-1"
      )
    }
  }

  private func makeNotification(revision: UInt64) throws -> CatalogConvergenceNotification {
    try CatalogConvergenceNotification(
      tenantID: CatalogTenantID("tenant-1"),
      domainID: domainID(),
      generation: 7,
      revision: revision,
      catalogRevision: revision + 100,
      sourceAuthority: CatalogSourceAuthorityID("source-main"),
      sourceRevision: revision,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogMutationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      affectedKeys: ["settings.json"],
      targets: [
        CatalogSignalTarget(
          kind: .container,
          parentID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        ),
        CatalogSignalTarget(kind: .workingSet),
      ]
    )
  }

  private func domainID(
    owner: String = "owner-1",
    account: String = "account-1"
  ) throws -> CatalogDomainID {
    try CatalogDomainID.derived(
      ownerID: CatalogOwnerID(owner),
      accountInstanceID: CatalogAccountInstanceID(account)
    )
  }

  private func registerDomain(_ system: RecordingDomainSystem) async throws {
    let ownerID = try CatalogOwnerID("owner-1")
    let accountID = try CatalogAccountInstanceID("account-1")
    _ = try await system.register(
      CatalogDomainRegistration(
        domainID: CatalogDomainID.derived(ownerID: ownerID, accountInstanceID: accountID),
        ownerID: ownerID,
        tenantID: CatalogTenantID("tenant-1"),
        generation: 7,
        rootID: rootID(),
        accountInstanceID: accountID,
        displayName: "Account 1"
      )
    )
  }

  private func domainRegistration() throws -> CatalogDomainRegistration {
    let owner = try CatalogOwnerID("owner-1")
    let account = try CatalogAccountInstanceID("account-1")
    return try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: account),
      ownerID: owner,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accountInstanceID: account,
      displayName: "Account 1"
    )
  }

  private func rootID() throws -> CatalogObjectID {
    try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
  }
}

private actor PublicationRecorder {
  private var values: [CatalogConvergenceNotification] = []

  func record(_ notification: CatalogConvergenceNotification) {
    values.append(notification)
  }

  func revisions() -> [UInt64] {
    values.map(\.revision)
  }
}

private enum DomainSystemTestError: Error, Equatable {
  case conflict
}

private actor RecordingDomainSystem: CatalogDomainSystem {
  private var signals: [(CatalogDomainID, CatalogSignalTarget)] = []
  private var domains: [CatalogDomainID: CatalogRegisteredDomain] = [:]
  private var cutovers: [CatalogDomainCutoverPlan] = []
  private var cutoverObservations: [CatalogDomainCutoverObservation] = []

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

  func list() async throws -> [CatalogRegisteredDomain] {
    Array(domains.values)
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    guard let domain = domains[binding.domainID],
          domain.tenantID == binding.tenantID,
          domain.generation == binding.generation
    else { throw DomainSystemTestError.conflict }
  }

  func signal(domainID: CatalogDomainID, target: CatalogSignalTarget) async throws {
    signals.append((domainID, target))
  }

  func cutover(_ plan: CatalogDomainCutoverPlan) async throws -> [CatalogDomainCutoverObservation] {
    cutovers.append(plan)
    return cutoverObservations
  }

  func setCutoverObservations(_ observations: [CatalogDomainCutoverObservation]) {
    cutoverObservations = observations
  }

  func cutoverCount() -> Int {
    cutovers.count
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

  func registrationCount() -> Int {
    domains.count
  }
}
