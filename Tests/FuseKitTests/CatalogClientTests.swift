import Foundation
@testable import FuseKit
import Testing

@Suite("Catalog protocol")
struct CatalogProtocolTests {
  @Test
  func generatedBuildIdentityIsApplicationSchemaDigest() {
    #expect(CatalogProtocol.version == 4)
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
  func preparationPreservesAllThreeRevisionSpaces() async throws {
    let transport = PreparationTransport()
    let tenant = try CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3)
    let notification = try notification(revision: 7)

    _ = try await CatalogClient(transport: transport).prepare(
      tenant: tenant,
      notification: notification
    )

    let request = try #require(await transport.request())
    #expect(request.catalogRevision == notification.catalogRevision)
    #expect(request.sourceRevision == notification.sourceRevision)
    #expect(request.changeID == notification.changeID)
    #expect(request.operationID == notification.operationID)
  }

  @Test
  func mismatchedAndConflictingNotificationsAreRejected() async throws {
    let transport = AckTransport()
    let binding = try binding()
    let inbox = CatalogConvergenceInbox(
      binding: binding,
      client: CatalogClient(transport: transport)
    )
    let accepted = try notification(revision: 7)
    try await inbox.receive(accepted)

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
      affectedKeys: accepted.affectedKeys,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.wrongDomain) {
      try await inbox.receive(otherDomain)
    }

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
      affectedKeys: accepted.affectedKeys,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.wrongGeneration) {
      try await inbox.receive(wrongGeneration)
    }

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
      affectedKeys: accepted.affectedKeys,
      targets: accepted.targets
    )
    await #expect(throws: CatalogConvergenceInbox.InboxError.conflictingNotification) {
      try await inbox.receive(conflict)
    }
  }

  private func binding() throws -> CatalogFileProviderBinding {
    try CatalogFileProviderBinding(
      domainID: testDomainID(),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      rootID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    )
  }

  private func notification(
    revision: UInt64,
    targets: [CatalogSignalTarget]? = nil
  ) throws -> CatalogConvergenceNotification {
    try CatalogConvergenceNotification(
      tenantID: CatalogTenantID("tenant-1"),
      domainID: testDomainID(),
      generation: 3,
      revision: revision,
      catalogRevision: revision + 100,
      sourceAuthority: CatalogSourceAuthorityID("source-main"),
      sourceRevision: revision,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogMutationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      affectedKeys: ["settings.json"],
      targets: targets ?? [workingSetTarget()]
    )
  }

  private func workingSetTarget() throws -> CatalogSignalTarget {
    try CatalogSignalTarget(kind: .workingSet)
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

private func testDomainID(
  owner: String = "owner-1",
  account: String = "account-1"
) throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID(owner),
    accountInstanceID: CatalogAccountInstanceID(account)
  )
}

private actor AckTransport: CatalogTransport {
  private var received: [CatalogAckConvergenceRequest] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    guard operation == .convergenceAck else {
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
    let request = try JSONDecoder().decode(CatalogAckConvergenceRequest.self, from: payload)
    received.append(request)
    return try JSONEncoder().encode(
      CatalogAckConvergenceResponse(
        code: .ok,
        message: "",
        observation: CatalogDomainObservation(
          tenantID: CatalogTenantID(tenant),
          domainID: request.domainID,
          generation: request.generation,
          requestedRevision: request.revision,
          observedRevision: request.revision,
          catalogRevision: request.catalogRevision,
          sourceAuthority: request.sourceAuthority,
          sourceRevision: request.sourceRevision,
          changeID: request.changeID,
          operationID: request.operationID
        )
      )
    )
  }

  func download(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }

  func acknowledgements() -> [CatalogAckConvergenceRequest] {
    received
  }
}

private actor ScopeTransport: CatalogTransport {
  private var received: [String] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload: Data) async throws -> Data {
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .catalogSnapshot:
      let request = try decoder.decode(CatalogSnapshotRequest.self, from: payload)
      received.append(
        "snapshot:\(request.generation):\(request.scope.kind.rawValue):\(request.scope.parentID?.rawValue ?? "")"
      )
      return try encoder.encode(
        CatalogSnapshotResponse(
          code: .ok,
          message: "",
          revision: request.revision,
          objects: []
        )
      )
    case .catalogChangesSince:
      let request = try decoder.decode(CatalogChangesSinceRequest.self, from: payload)
      received.append(
        "changes:\(request.generation):\(request.scope.kind.rawValue):\(request.scope.parentID?.rawValue ?? "")"
      )
      return try encoder.encode(
        CatalogChangesSinceResponse(
          code: .ok,
          message: "",
          floor: 1,
          head: 7,
          next: CatalogChangeCursor(
            revision: 7,
            sequence: CatalogProtocol.changeCursorCompleteSequence
          ),
          complete: true,
          changes: []
        )
      )
    default:
      throw CatalogTransportError.remote("unexpected operation")
    }
  }

  func download(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }

  func scopes() -> [String] {
    received
  }
}

private actor PreparationTransport: CatalogTransport {
  private var received: CatalogPrepareTenantRequest?

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    guard operation == .tenantPrepare else {
      throw CatalogTransportError.remote("unexpected operation")
    }
    let request = try JSONDecoder().decode(CatalogPrepareTenantRequest.self, from: payload)
    received = request
    let tenantID = try CatalogTenantID(tenant)
    return try JSONEncoder().encode(
      CatalogPrepareTenantResponse(
        code: .ok,
        message: "",
        proof: CatalogPreparationProof(
          catalog: CatalogLaneProof(
            tenant: tenantID,
            generation: request.generation,
            requested: request.catalogRevision,
            desired: request.catalogRevision,
            observed: request.catalogRevision,
            verified: request.catalogRevision,
            applied: request.catalogRevision
          ),
          domain: CatalogDomainObservation(
            tenantID: tenantID,
            domainID: request.domainID,
            generation: request.generation,
            requestedRevision: 7,
            observedRevision: 7,
            catalogRevision: request.catalogRevision,
            sourceAuthority: request.sourceAuthority,
            sourceRevision: request.sourceRevision,
            changeID: request.changeID,
            operationID: request.operationID
          )
        )
      )
    )
  }

  func download(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }

  func request() -> CatalogPrepareTenantRequest? {
    received
  }
}
