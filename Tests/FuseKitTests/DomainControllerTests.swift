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

    let firstResult = await controller.execute(first)
    let duplicateResult = await controller.execute(duplicate)

    #expect(firstResult.code == .ok)
    #expect(duplicateResult.code == .ok)
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
    let domain = try await registerScaleDomains(system)
    let targets = try scaleTargets()
    let notification = try testActivationNotification(
      tenantID: domain.tenantID, domainID: domain.domainID, generation: domain.generation,
      activationRevision: 1, catalogHead: 1,
      targetCount: UInt64(targets.count),
      targets: targets
    )
    let result = try await CatalogDomainController(system: system).execute(
      CatalogBrokerCommand(commandID: 1, kind: .signalDomain, notification: notification)
    )
    #expect(result.code == .ok)
    #expect(await system.signalCallCount() == 1)
    #expect(await system.signalKeys().count == targets.count)
  }

  @Test
  func staleNotificationIsRejectedWithoutSignaling() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    try await registerDomain(system)
    for revision in [12, 11] {
      let notification = try makeNotification(revision: UInt64(revision))
      let command = try CatalogBrokerCommand(
        commandID: UInt64(revision),
        kind: .signalDomain,
        notification: notification
      )
      let result = await controller.execute(command)
      if revision == 11 {
        #expect(result.code == .invalidRequest)
      }
    }

    #expect(await system.signalKeys().count == 2)
  }

  @Test
  func registrationReplayIsIdempotentAndMetadataDriftConflicts() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let registration = try domainRegistration()
    for commandID: UInt64 in [1, 2] {
      let result = try await controller.execute(CatalogBrokerCommand(
        commandID: commandID,
        kind: .registerDomain,
        registration: registration
      ))
      #expect(result.code == .ok)
      #expect(result.registered?.generation == 7)
    }

    let conflict = try await controller.execute(CatalogBrokerCommand(
      commandID: 3,
      kind: .registerDomain,
      registration: driftedRegistration(
        registration,
        rootID: CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
      )
    ))
    #expect(conflict.code == .unavailable)
    let accessConflict = try await controller.execute(CatalogBrokerCommand(
      commandID: 4,
      kind: .registerDomain,
      registration: driftedRegistration(registration, accessMode: .readOnly)
    ))
    #expect(accessConflict.code == .unavailable)
    #expect(await system.registrationCount() == 1)
  }

  @Test
  func criticalMaterializationSchedulesExactObjectsAndReturnsSortedPaths() async throws {
    let system = RecordingDomainSystem()
    try await registerDomain(system)
    let readiness = try criticalReadinessProof()
    let command = try CatalogBrokerCommand(
      commandID: 1,
      kind: .materializeCritical,
      criticalReadiness: readiness
    )

    let result = await CatalogDomainController(system: system).execute(command)

    #expect(result.code == .ok)
    #expect(result.materializationScheduled == true)
    #expect(
      result.materializationPaths?.map(\.objectID.rawValue) == [
        "11111111111111111111111111111111",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      ]
    )
    #expect(await system.materializationCount() == 1)
  }

  @Test
  func criticalMaterializationRejectsInvalidFenceBeforeScheduling() async throws {
    let system = RecordingDomainSystem()
    try await registerDomain(system)
    let valid = try criticalReadinessProof()
    let invalid = CatalogCriticalReadinessProof(
      policyDigest: valid.policyDigest,
      resolutionDigest: valid.resolutionDigest,
      catalogHead: 0,
      sourceRevision: valid.sourceRevision,
      tenantGeneration: valid.tenantGeneration,
      domainID: valid.domainID,
      presentationInstanceID: valid.presentationInstanceID,
      rootID: valid.rootID,
      activationGeneration: valid.activationGeneration,
      lease: valid.lease,
      objects: valid.objects
    )
    let command = try CatalogBrokerCommand(
      commandID: 1,
      kind: .materializeCritical,
      criticalReadiness: invalid
    )

    let result = await CatalogDomainController(system: system).execute(command)

    #expect(result.code == .invalidRequest)
    #expect(await system.materializationCount() == 0)
  }

  @Test
  func criticalSchedulingRejectsCompletedReadProofAndNoncanonicalPaths() throws {
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogBrokerCommand(
        commandID: 1,
        kind: .materializeCritical,
        criticalReadiness: criticalReadinessProof(
          readProofDigest: String(repeating: "f", count: 64)
        )
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogCriticalMaterializationPath(
        objectID: CatalogObjectID("11111111111111111111111111111111"),
        path: "/tmp/../tmp/critical"
      )
    }
  }
}

private func registerScaleDomains(
  _ system: RecordingDomainSystem
) async throws -> CatalogRegisteredDomain {
  let owner = try CatalogOwnerID("owner-scale")
  var selected: CatalogRegisteredDomain?
  for index in 0..<100 {
    let account = try CatalogPresentationInstanceID(String(format: "account-%03d", index))
    let registered = try await system.register(
      CatalogDomainRegistration(
        domainID: CatalogDomainID.derived(ownerID: owner, presentationInstanceID: account),
        ownerID: owner,
        tenantID: CatalogTenantID(String(format: "tenant-%03d", index)),
        generation: 1,
        rootID: rootID(),
        accessMode: .readWrite,
        presentationInstanceID: account,
        displayName: "Scale"
      )
    )
    if index == 0 {
      selected = registered
    }
  }
  return try #require(selected)
}

private func scaleTargets() throws -> [CatalogSignalTarget] {
  var targets = try (1..<Int(CatalogProtocol.maxSignalTargets)).map {
    try CatalogSignalTarget(
      kind: .container,
      parentID: CatalogObjectID(String(format: "%032x", $0))
    )
  }
  try targets.append(CatalogSignalTarget(kind: .workingSet))
  return targets
}

private func driftedRegistration(
  _ registration: CatalogDomainRegistration,
  rootID: CatalogObjectID? = nil,
  accessMode: CatalogTenantAccessMode? = nil
) throws -> CatalogDomainRegistration {
  try CatalogDomainRegistration(
    domainID: registration.domainID,
    ownerID: registration.ownerID,
    tenantID: registration.tenantID,
    generation: registration.generation,
    rootID: rootID ?? registration.rootID,
    accessMode: accessMode ?? registration.accessMode,
    presentationInstanceID: registration.presentationInstanceID,
    displayName: registration.displayName
  )
}
