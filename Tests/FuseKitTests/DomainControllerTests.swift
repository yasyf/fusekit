@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

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
    let domain = try await registerScaleDomains(system)
    let targets = try scaleTargets()
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
    let result = try await CatalogDomainController(system: system).execute(
      CatalogBrokerCommand(commandID: 1, kind: .signalDomain, notification: notification),
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
    let registration = try domainRegistration()
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
        registration: driftedRegistration(
          registration,
          rootID: CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        )
      ),
      publish: { _ in }
    )
    #expect(conflict.code == .unavailable)
    let accessConflict = try await controller.execute(
      CatalogBrokerCommand(
        commandID: 4,
        kind: .registerDomain,
        registration: driftedRegistration(registration, accessMode: .readOnly)
      ),
      publish: { _ in }
    )
    #expect(accessConflict.code == .unavailable)
    #expect(await system.registrationCount() == 1)
  }
}

private func registerScaleDomains(
  _ system: RecordingDomainSystem
) async throws -> CatalogRegisteredDomain {
  let owner = try CatalogOwnerID("owner-scale")
  var selected: CatalogRegisteredDomain?
  for index in 0 ..< 100 {
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
  var targets = try (1 ..< Int(CatalogProtocol.maxSignalTargets)).map {
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
