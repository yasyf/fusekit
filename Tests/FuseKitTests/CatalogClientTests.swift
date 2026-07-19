import Foundation
import Testing

@testable import FuseKit

@Suite("Catalog protocol")
struct CatalogProtocolTests {
  @Test
  func generatedBuildIdentityIsApplicationSchemaDigest() {
    #expect(CatalogProtocol.version == 1)
    #expect(FuseKitTransportProtocol.daemonkitBuild.hasPrefix("fusekit.transport."))
    #expect(FuseKitTransportProtocol.daemonkitBuild.count == "fusekit.transport.".count + 64)
  }

  @Test
  func zeroTenantGenerationIsRejectedLocally() throws {
    #expect(throws: CatalogClientError.invalidGeneration) {
      _ = try CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 0)
    }
  }

  @Test
  func catalogObjectSizeMustFitSignedPresentationRange() throws {
    let oversized = Data(
      """
      {
        "id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "parent_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "revision":2,
        "metadata_revision":2,
        "content_revision":2,
        "name":"large",
        "kind":"file",
        "mode":384,
        "size":9223372036854775808,
        "hash":"0000000000000000000000000000000000000000000000000000000000000000",
        "link_target":"",
        "desired":2,
        "observed":2,
        "verified":2,
        "applied":2,
        "tombstone":false
      }
      """.utf8)
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try JSONDecoder().decode(CatalogObject.self, from: oversized)
    }
  }

  @Test
  func namesHaveExactPortableUTF8Bound() throws {
    func objectData(name: String) -> Data {
      Data(
        """
        {
          "id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          "parent_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "revision":2,
          "metadata_revision":2,
          "content_revision":0,
          "name":"\(name)",
          "kind":"directory",
          "mode":493,
          "size":0,
          "hash":"",
          "link_target":"",
          "desired":2,
          "observed":2,
          "verified":2,
          "applied":2,
          "tombstone":false
        }
        """.utf8)
    }

    _ = try JSONDecoder().decode(
      CatalogObject.self,
      from: objectData(name: String(repeating: "a", count: Int(CatalogProtocol.maxNameBytes)))
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try JSONDecoder().decode(
        CatalogObject.self,
        from: objectData(name: String(repeating: "a", count: Int(CatalogProtocol.maxNameBytes) + 1))
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try JSONDecoder().decode(CatalogObject.self, from: objectData(name: #"bad\u0001name"#))
    }
  }

  @Test
  func snapshotAndChangesCarryClosedServerSideScope() async throws {
    let transport = ScopeTransport()
    let client = CatalogClient(transport: transport)
    let tenant = try CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3)
    let parent = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let container = try CatalogEnumerationScope(kind: .container, parentID: parent)
    let workingSet = try CatalogEnumerationScope(kind: .workingSet)

    _ = try await client.snapshot(
      tenant: tenant,
      revision: 7,
      scope: container,
      limit: 10
    )
    _ = try await client.changes(
      tenant: tenant,
      since: CatalogChangeCursor(
        revision: 6,
        sequence: CatalogProtocol.changeCursorCompleteSequence
      ),
      scope: workingSet,
      limit: 10
    )

    #expect(
      await transport.scopes() == [
        "snapshot:3:container:\(parent.rawValue)",
        "changes:3:working_set:",
      ]
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogEnumerationScope(kind: .container)
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogEnumerationScope(kind: .workingSet, parentID: parent)
    }
  }

  @Test
  func snapshotRejectsMalformedImmutablePages() async throws {
    let first = try snapshotObject(id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    let second = try snapshotObject(id: "cccccccccccccccccccccccccccccccc")
    let after = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let tombstone = try snapshotObject(
      id: "dddddddddddddddddddddddddddddddd",
      tombstone: true
    )
    let future = try snapshotObject(
      id: "dddddddddddddddddddddddddddddddd",
      revision: 8
    )
    let pages = [
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [first, second], next: second.id
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [tombstone]
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [second, first]
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [first, first]
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [future]
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [], next: first.id
      ),
      CatalogSnapshotResponse(
        code: .ok, message: "", revision: 7, objects: [first], next: second.id
      ),
    ]

    for page in pages {
      let client = CatalogClient(transport: SnapshotTransport(response: page))
      await #expect(throws: CatalogClientError.self) {
        try await client.snapshot(
          tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
          revision: 7,
          scope: CatalogEnumerationScope(kind: .workingSet),
          after: after,
          limit: 1
        )
      }
    }
  }

  @Test
  func snapshotAcceptsStrictPageWithExactContinuation() async throws {
    let first = try snapshotObject(id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    let second = try snapshotObject(id: "cccccccccccccccccccccccccccccccc")
    let response = CatalogSnapshotResponse(
      code: .ok, message: "", revision: 7, objects: [first, second], next: second.id
    )

    let page = try await CatalogClient(transport: SnapshotTransport(response: response)).snapshot(
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      revision: 7,
      scope: CatalogEnumerationScope(kind: .workingSet),
      after: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
      limit: 2
    )

    #expect(page.objects.map(\.id) == [first.id, second.id])
    #expect(page.next == second.id)
  }

  @Test
  func openRejectsTerminalMetadataForAnotherRevision() async throws {
    let requested = try CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    let mismatched = try snapshotObject(id: requested.rawValue, revision: 8)
    let download = try await CatalogClient(
      transport: OpenTransport(object: mismatched)
    ).open(
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      objectID: requested,
      revision: 7
    )

    #expect(try await download.next() == nil)
    await #expect(
      throws: CatalogClientError.response(
        .integrity,
        "stream metadata does not match request"
      )
    ) {
      try await download.response()
    }
  }

  @Test
  func crossLanguageGoldenMessagesRoundTripCanonically() throws {
    let repository = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
    let fixture = repository.appendingPathComponent("catalogproto/testdata/golden.json")
    let root = try #require(
      JSONSerialization.jsonObject(with: Data(contentsOf: fixture)) as? [String: Any]
    )
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let decoder = JSONDecoder()

    try expectCanonicalRoundTrip(
      root["head_response"],
      type: CatalogHeadResponse.self,
      encoder: encoder,
      decoder: decoder
    )
    try expectCanonicalRoundTrip(
      root["mutation_request"],
      type: CatalogMutationRequest.self,
      encoder: encoder,
      decoder: decoder
    )
    try expectCanonicalRoundTrip(
      root["broker_command"],
      type: CatalogBrokerCommand.self,
      encoder: encoder,
      decoder: decoder
    )
  }
}

extension CatalogProtocolTests {
  @Test
  func acknowledgementPreservesExactCausalTuple() async throws {
    let transport = AckTransport()
    let client = CatalogClient(transport: transport)
    let binding = try binding()
    let inbox = CatalogConvergenceInbox(binding: binding, client: client)
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
    #expect(acknowledgements[0].revision == notification.revision)
    #expect(acknowledgements[0].catalogRevision == notification.catalogRevision)
    #expect(acknowledgements[0].sourceRevision == notification.sourceRevision)
    #expect(acknowledgements[0].changeID == notification.changeID)
    #expect(acknowledgements[0].operationID == notification.operationID)
  }

  @Test
  func newerNotificationSupersedesUnobservedRevision() async throws {
    let transport = AckTransport()
    let client = CatalogClient(transport: transport)
    let binding = try binding()
    let inbox = CatalogConvergenceInbox(binding: binding, client: client)
    let older = try notification(revision: 7)
    let newer = try notification(revision: 9)

    try await inbox.receive(older)
    try await inbox.receive(newer)
    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 108)
    #expect(await transport.acknowledgements().isEmpty)

    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 109)
    let acknowledgements = await transport.acknowledgements()
    #expect(acknowledgements.map(\.revision) == [9])
  }

  @Test
  func notificationArrivingAfterDeltaObservationIsAcknowledged() async throws {
    let transport = AckTransport()
    let binding = try binding()
    let inbox = CatalogConvergenceInbox(
      binding: binding,
      client: CatalogClient(transport: transport)
    )

    try await inbox.acknowledgeObserved(target: workingSetTarget(), upTo: 107)
    try await inbox.receive(notification(revision: 7))

    #expect(await transport.acknowledgements().map(\.revision) == [7])
  }

  @Test
  func everySignaledScopeMustObserveCatalogRevisionBeforeAck() async throws {
    let parent = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let container = try CatalogSignalTarget(kind: .container, parentID: parent)
    let workingSet = try workingSetTarget()
    for first in [container, workingSet] {
      let transport = AckTransport()
      let inbox = try CatalogConvergenceInbox(
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
      #expect(await transport.acknowledgements().map(\.revision) == [7])
    }
  }

  @Test
  func preparationSplitsTenantProofFromExactDomainObservation() async throws {
    let transport = PreparationTransport()
    let tenant = try CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3)
    let domainID = try testDomainID()

    let client = CatalogClient(transport: transport)
    let proof = try await client.prepareTenant(tenant: tenant)
    _ = try await client.prepareDomain(tenant: tenant, domainID: domainID, proof: proof)

    let tenantRequest = try #require(await transport.tenantRequest())
    #expect(tenantRequest.generation == tenant.generation)
    let domainRequest = try #require(await transport.domainRequest())
    #expect(domainRequest.domainID == domainID)
    #expect(domainRequest.generation == tenant.generation)
    #expect(domainRequest.catalogRevision == proof.catalogRevision)
    #expect(domainRequest.sourceAuthority == proof.sourceAuthority)
    #expect(domainRequest.sourceRevision == proof.sourceRevision)
    #expect(domainRequest.changeID == proof.changeID)
    #expect(domainRequest.operationID == proof.operationID)
  }

  @Test
  func domainPreparationRejectsProofForAnotherTenantBeforeTransport() async throws {
    let transport = PreparationTransport()
    let preparedTenant = try CatalogTenant(
      identifier: CatalogTenantID("tenant-1"), generation: 3)
    let requestedTenant = try CatalogTenant(
      identifier: CatalogTenantID("tenant-2"), generation: 3)
    let client = CatalogClient(transport: transport)
    let proof = try await client.prepareTenant(tenant: preparedTenant)

    await #expect(throws: CatalogClientError.self) {
      try await client.prepareDomain(
        tenant: requestedTenant,
        domainID: testDomainID(),
        proof: proof
      )
    }
    #expect(await transport.domainRequest() == nil)
  }

  @Test
  func notificationForAnotherDomainIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let otherDomain = try CatalogConvergenceNotification(
      tenantID: accepted.tenantID,
      domainID: testDomainID(owner: "other-owner", account: "other-account"),
      generation: accepted.generation,
      revision: accepted.revision,
      catalogRevision: accepted.catalogRevision,
      sourceAuthority: accepted.sourceAuthority,
      sourceRevision: accepted.sourceRevision,
      changeID: accepted.changeID,
      operationID: accepted.operationID,
      cause: accepted.cause,
      originDomain: accepted.originDomain,
      originGeneration: accepted.originGeneration,
      fingerprint: accepted.fingerprint,
      affectedCount: accepted.affectedCount,
      affectedDigest: accepted.affectedDigest,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.wrongDomain) {
      try await inbox.receive(otherDomain)
    }
  }

  @Test
  func notificationForAnotherGenerationIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let wrongGeneration = CatalogConvergenceNotification(
      tenantID: accepted.tenantID,
      domainID: accepted.domainID,
      generation: accepted.generation + 1,
      revision: accepted.revision + 1,
      catalogRevision: accepted.catalogRevision + 1,
      sourceAuthority: accepted.sourceAuthority,
      sourceRevision: accepted.sourceRevision + 1,
      changeID: accepted.changeID,
      operationID: accepted.operationID,
      cause: accepted.cause,
      originDomain: accepted.originDomain,
      originGeneration: accepted.originGeneration,
      fingerprint: accepted.fingerprint,
      affectedCount: accepted.affectedCount,
      affectedDigest: accepted.affectedDigest,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.wrongGeneration) {
      try await inbox.receive(wrongGeneration)
    }
  }

  @Test
  func conflictingReplayIsRejected() async throws {
    let (inbox, accepted) = try await acceptedInbox()
    let conflict = try CatalogConvergenceNotification(
      tenantID: accepted.tenantID,
      domainID: accepted.domainID,
      generation: accepted.generation,
      revision: accepted.revision,
      catalogRevision: accepted.catalogRevision,
      sourceAuthority: accepted.sourceAuthority,
      sourceRevision: accepted.sourceRevision,
      changeID: CatalogChangeID("33333333333333333333333333333333"),
      operationID: accepted.operationID,
      cause: accepted.cause,
      originDomain: accepted.originDomain,
      originGeneration: accepted.originGeneration,
      fingerprint: accepted.fingerprint,
      affectedCount: accepted.affectedCount,
      affectedDigest: accepted.affectedDigest,
      targetCount: accepted.targetCount,
      targetDigest: accepted.targetDigest,
      targetsCoalesced: accepted.targetsCoalesced,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.conflictingNotification) {
      try await inbox.receive(conflict)
    }
  }

  private func acceptedInbox() async throws
    -> (CatalogConvergenceInbox, CatalogConvergenceNotification)
  {
    let binding = try binding()
    let inbox = CatalogConvergenceInbox(
      binding: binding,
      client: CatalogClient(transport: AckTransport())
    )
    let accepted = try notification(revision: 7)
    try await inbox.receive(accepted)
    return (inbox, accepted)
  }

  private func binding() throws -> CatalogFileProviderBinding {
    try CatalogFileProviderBinding(
      domainID: testDomainID(),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      rootID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
      accessMode: .readWrite
    )
  }

  private func notification(
    revision: UInt64,
    targets: [CatalogSignalTarget]? = nil
  ) throws -> CatalogConvergenceNotification {
    let signalTargets = try targets ?? [workingSetTarget()]
    return try CatalogConvergenceNotification(
      tenantID: CatalogTenantID("tenant-1"),
      domainID: testDomainID(),
      generation: 3,
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
      targetCount: UInt64(signalTargets.count),
      targetDigest: String(repeating: "b", count: 64),
      targetsCoalesced: false,
      targets: signalTargets
    )
  }

  private func workingSetTarget() throws -> CatalogSignalTarget {
    try CatalogSignalTarget(kind: .workingSet)
  }

  private func snapshotObject(
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

  private func expectCanonicalRoundTrip(
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
