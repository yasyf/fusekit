import Foundation
@testable import FuseKit
import Testing

@Suite("Catalog protocol")
struct CatalogProtocolTests {
  @Test
  func generatedBuildIdentityIsApplicationSchemaDigest() {
    #expect(CatalogProtocol.version == 1)
    #expect(FuseKitTransportProtocol.wireBuild.hasPrefix("com.yasyf.fusekit.transport/"))
    #expect(FuseKitTransportProtocol.wireBuild.hasSuffix("/v1"))
    #expect(
      FuseKitTransportProtocol.wireBuild.count
        == "com.yasyf.fusekit.transport/".count + 64 + "/v1".count
    )
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
      """.utf8
    )
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
        """.utf8
      )
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
