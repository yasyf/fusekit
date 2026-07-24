import Foundation
@testable import FuseKit
import Testing

extension CatalogProtocolTests {
  @Test
  func acknowledgementPreservesExactCausalTuple() async throws {
    let transport = AckTransport()
    let client = CatalogClient(transport: transport)
    let binding = try binding()
    let inbox = CatalogActivationInbox(binding: binding, client: client)
    let notification = try notification(revision: 7)

    try await inbox.receive(notification)
    try await inbox.receive(notification)
    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 106)
    #expect(await transport.acknowledgements().isEmpty)

    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 107)
    let acknowledgements = await transport.acknowledgements()
    #expect(acknowledgements.count == 1)
    #expect(acknowledgements[0].domainID == notification.domainID)
    #expect(acknowledgements[0].generation == binding.tenant.generation)
    #expect(acknowledgements[0].activationChangeID == notification.activationChangeID)
    #expect(acknowledgements[0].activationRevision == notification.activationRevision)
    #expect(acknowledgements[0].catalogHead == notification.catalogHead)
    #expect(acknowledgements[0].headDigest == notification.headDigest)
  }

  @Test
  func newerNotificationSupersedesUnobservedRevision() async throws {
    let transport = AckTransport()
    let client = CatalogClient(transport: transport)
    let binding = try binding()
    let inbox = CatalogActivationInbox(binding: binding, client: client)
    let older = try notification(revision: 7)
    let newer = try notification(revision: 9)

    try await inbox.receive(older)
    try await inbox.receive(newer)
    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 108)
    #expect(await transport.acknowledgements().isEmpty)

    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 109)
    let acknowledgements = await transport.acknowledgements()
    #expect(acknowledgements.map(\.activationRevision) == [9])
  }

  @Test
  func notificationArrivingAfterDeltaObservationIsAcknowledged() async throws {
    let transport = AckTransport()
    let binding = try binding()
    let inbox = CatalogActivationInbox(
      binding: binding,
      client: CatalogClient(transport: transport)
    )

    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 107)
    try await inbox.receive(notification(revision: 7))

    #expect(await transport.acknowledgements().map(\.activationRevision) == [7])
  }

  @Test
  func everySignaledScopeMustObserveCatalogRevisionBeforeAck() async throws {
    let parent = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let container = try CatalogSignalTarget(kind: .container, parentID: parent)
    let workingSet = try workingSetTarget()
    for first in [container, workingSet] {
      let transport = AckTransport()
      let inbox = try CatalogActivationInbox(
        binding: binding(),
        client: CatalogClient(transport: transport)
      )
      let notification = try notification(
        revision: 7,
        targets: [container, workingSet]
      )
      try await inbox.receive(notification)

      try await inbox.acknowledgeObserved(target: first, upTo: 107)
      #expect(await transport.acknowledgements().isEmpty)
      let second = first.kind == .container ? workingSet : container
      try await inbox.acknowledgeObserved(target: second, upTo: 107)
      #expect(await transport.acknowledgements().map(\.activationRevision) == [7])
    }
  }

  @Test
  func notificationForAnotherDomainIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let otherDomain = try CatalogActivationNotification(
      activationChangeID: accepted.activationChangeID,
      tenantID: accepted.tenantID,
      domainID: testDomainID(owner: "other-owner", account: "other-account"),
      generation: accepted.generation,
      activationRevision: accepted.activationRevision,
      catalogHead: accepted.catalogHead,
      headDigest: accepted.headDigest,
      providerFingerprint: accepted.providerFingerprint,
      causes: accepted.causes,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogActivationInbox.InboxError.wrongDomain) {
      try await inbox.receive(otherDomain)
    }
  }

  @Test
  func notificationForAnotherGenerationIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let wrongGeneration = CatalogActivationNotification(
      activationChangeID: accepted.activationChangeID,
      tenantID: accepted.tenantID,
      domainID: accepted.domainID,
      generation: accepted.generation + 1,
      activationRevision: accepted.activationRevision + 1,
      catalogHead: accepted.catalogHead + 1,
      headDigest: accepted.headDigest,
      providerFingerprint: accepted.providerFingerprint,
      causes: accepted.causes,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogActivationInbox.InboxError.wrongGeneration) {
      try await inbox.receive(wrongGeneration)
    }
  }

  @Test
  func conflictingReplayIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let conflict = try CatalogActivationNotification(
      activationChangeID: CatalogActivationChangeID("44444444444444444444444444444444"),
      tenantID: accepted.tenantID,
      domainID: accepted.domainID,
      generation: accepted.generation,
      activationRevision: accepted.activationRevision,
      catalogHead: accepted.catalogHead,
      headDigest: accepted.headDigest,
      providerFingerprint: accepted.providerFingerprint,
      causes: accepted.causes,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogActivationInbox.InboxError.conflictingNotification) {
      try await inbox.receive(conflict)
    }
  }

  func acceptedInbox() async throws
    -> (CatalogActivationInbox, CatalogActivationNotification) {
    let binding = try binding()
    let inbox = CatalogActivationInbox(
      binding: binding,
      client: CatalogClient(transport: AckTransport())
    )
    let accepted = try notification(revision: 7)
    try await inbox.receive(accepted)
    return (inbox, accepted)
  }

  func binding() throws -> CatalogFileProviderBinding {
    try CatalogFileProviderBinding(
      domainID: testDomainID(),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      rootID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
      accessMode: .readWrite
    )
  }

  func notification(
    revision: UInt64,
    targets: [CatalogSignalTarget]? = nil
  ) throws -> CatalogActivationNotification {
    let signalTargets = try targets ?? [workingSetTarget()]
    return try testActivationNotification(
      tenantID: CatalogTenantID("tenant-1"), domainID: testDomainID(), generation: 3,
      activationRevision: revision, catalogHead: revision + 100, sourceRevision: revision,
      targetCount: UInt64(signalTargets.count),
      targets: signalTargets
    )
  }

  func workingSetTarget() throws -> CatalogSignalTarget {
    try CatalogSignalTarget(kind: .workingSet)
  }

  func snapshotObject(
    id: String,
    revision: UInt64 = 7,
    tombstone: Bool = false
  ) throws -> CatalogObject {
    try CatalogObject(
      id: CatalogObjectID(id),
      parentID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
      revision: revision,
      metadataRevision: revision,
      contentRevision: 1,
      name: "\(id).json",
      kind: .file,
      mode: 0o644,
      size: 0,
      hash: "",
      linkTarget: "",
      desired: revision,
      observed: revision,
      verified: revision,
      applied: revision,
      tombstone: tombstone
    )
  }

  func expectCanonicalRoundTrip(
    _ raw: Any?,
    type: (some Codable).Type,
    encoder: JSONEncoder,
    decoder: JSONDecoder
  ) throws {
    let raw = try #require(raw)
    let canonical = try JSONSerialization.data(withJSONObject: raw, options: [.sortedKeys])
    let decoded = try decoder.decode(type, from: canonical)
    #expect(try encoder.encode(decoded) == canonical)
  }
}
