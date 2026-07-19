import DaemonKit
import FuseKit
import Testing

@Suite("Signed App Group endpoint")
struct AppGroupEndpointTests {
  @Test func canonicalTeamPrefixedIdentifierIsPreserved() throws {
    let endpoint = try CatalogAppGroupEndpoint(
      identifier: "SXKCTF23Q2.ccp",
      socketLeaf: "fusekit.sock"
    )
    #expect(endpoint.identifier == "SXKCTF23Q2.ccp")
    #expect(endpoint.socketLeaf == "fusekit.sock")
    let same = try CatalogAppGroupEndpoint(
      identifier: "SXKCTF23Q2.ccp",
      socketLeaf: "fusekit.sock"
    )
    let different = try CatalogAppGroupEndpoint(
      identifier: "SXKCTF23Q2.ccp",
      socketLeaf: "other.sock"
    )
    #expect(endpoint == same)
    #expect(endpoint != different)
  }

  @Test func invalidIdentifierIsRejectedBeforeContainerResolution() {
    #expect(throws: AppGroupContainer.ContainerError.invalidIdentifier("not-a-group")) {
      _ = try CatalogAppGroupEndpoint(identifier: "not-a-group", socketLeaf: "fusekit.sock")
    }
  }

  @Test func invalidLeafIsRejectedBeforeContainerResolution() {
    #expect(throws: AppGroupContainer.ContainerError.invalidLeaf("../fusekit.sock")) {
      _ = try CatalogAppGroupEndpoint(
        identifier: "group.com.example.product",
        socketLeaf: "../fusekit.sock"
      )
    }
  }
}
