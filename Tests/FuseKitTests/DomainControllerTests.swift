@preconcurrency import FileProvider
import Foundation
import Testing

@testable import FuseKit

@Suite("Domain signaling")
struct DomainControllerTests {
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
    #expect(await system.signalCallCount() == 1)
  }

  @Test
  func oneHundredDomainsAndManyTargetsUseOneIndexedSignalOperation() async throws {
    let system = RecordingDomainSystem()
    let owner = try CatalogOwnerID("owner-scale")
    var selected: CatalogRegisteredDomain?
    for index in 0..<100 {
      let account = try CatalogAccountInstanceID(String(format: "account-%03d", index))
      let registered = try await system.register(
        CatalogDomainRegistration(
          domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: account),
          ownerID: owner,
          tenantID: CatalogTenantID(String(format: "tenant-%03d", index)),
          generation: 1,
          rootID: rootID(),
          accessMode: .readWrite,
          accountInstanceID: account,
          displayName: "Scale"
        )
      )
      if index == 0 { selected = registered }
    }
    let domain = try #require(selected)
    var targets = try (1..<Int(CatalogProtocol.maxSignalTargets)).map {
      try CatalogSignalTarget(
        kind: .container,
        parentID: try CatalogObjectID(String(format: "%032x", $0))
      )
    }
    targets.append(try CatalogSignalTarget(kind: .workingSet))
    let notification = try CatalogConvergenceNotification(
      tenantID: domain.tenantID,
      domainID: domain.domainID,
      generation: domain.generation,
      revision: 1,
      catalogRevision: 1,
      sourceAuthority: CatalogSourceAuthorityID("source-scale"),
      sourceRevision: 1,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogOperationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      originGeneration: 0,
      fingerprint: String(repeating: "c", count: 64),
      affectedCount: UInt64(targets.count),
      affectedDigest: String(repeating: "a", count: 64),
      targetCount: UInt64(targets.count),
      targetDigest: String(repeating: "b", count: 64),
      targetsCoalesced: false,
      targets: targets
    )
    let result = await CatalogDomainController(system: system).execute(
      try CatalogBrokerCommand(commandID: 1, kind: .signalDomain, notification: notification),
      publish: { _ in }
    )
    #expect(result.code == .ok)
    #expect(await system.signalCallCount() == 1)
    #expect(await system.signalKeys().count == targets.count)
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
      accessMode: .readWrite,
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
          accessMode: registration.accessMode,
          accountInstanceID: registration.accountInstanceID,
          displayName: registration.displayName
        )
      ),
      publish: { _ in }
    )
    #expect(conflict.code == .unavailable)
    let accessConflict = try await controller.execute(
      CatalogBrokerCommand(
        commandID: 4,
        kind: .registerDomain,
        registration: CatalogDomainRegistration(
          domainID: registration.domainID,
          ownerID: registration.ownerID,
          tenantID: registration.tenantID,
          generation: registration.generation,
          rootID: registration.rootID,
          accessMode: .readOnly,
          accountInstanceID: registration.accountInstanceID,
          displayName: registration.displayName
        )
      ),
      publish: { _ in }
    )
    #expect(accessConflict.code == .unavailable)
    #expect(await system.registrationCount() == 1)
  }
}

extension DomainControllerTests {
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
    #expect(binding.accessMode == registration.accessMode)
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
    var badAccess = CatalogDomainMetadata(registration: registration).userInfo
    let accessKey = try #require(
      badAccess.first(where: { $0.value == registration.accessMode.rawValue })?.key
    )
    badAccess[accessKey] = "unknown"
    domain.userInfo = badAccess
    #expect(throws: CatalogDomainMetadataError.missing) {
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
      accessMode: .readWrite,
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
  func listDomainsUsesStrictBoundedContinuationPages() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let owner = try CatalogOwnerID("owner-page")
    for index in 0...Int(CatalogProtocol.maxBrokerDomainPageSize) {
      let account = try CatalogAccountInstanceID(String(format: "account-%03d", index))
      _ = try await system.register(
        CatalogDomainRegistration(
          domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: account),
          ownerID: owner,
          tenantID: CatalogTenantID(String(format: "tenant-%03d", index)),
          generation: 1,
          rootID: rootID(),
          accessMode: .readWrite,
          accountInstanceID: account,
          displayName: "Page"
        )
      )
    }
    let first = await controller.execute(
      try CatalogBrokerCommand(commandID: 1, kind: .listDomains),
      publish: { _ in }
    )
    #expect(first.domains?.count == Int(CatalogProtocol.maxBrokerDomainPageSize))
    let cursor = try #require(first.nextAfterDomainID)
    #expect(first.domains?.last?.domainID == cursor)

    let final = await controller.execute(
      try CatalogBrokerCommand(
        commandID: 2,
        kind: .listDomains,
        afterDomainID: cursor
      ),
      publish: { _ in }
    )
    #expect(final.domains?.count == 1)
    #expect(final.nextAfterDomainID == nil)
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
        accessMode: .readWrite,
        accountInstanceID: account,
        displayName: "Account 1",
        publicPath: "/tmp/account-1"
      )
    }
  }
}
