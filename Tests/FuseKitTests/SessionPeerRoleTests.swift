@testable import FuseKit
import Testing

@Suite("Session peer roles")
struct SessionPeerRoleTests {
  @Test func rolesMatchTheDaemonTrustPolicy() {
    #expect(FuseKitSessionPeerRole.broker == "fusekit.broker.v1")
    #expect(
      FuseKitSessionPeerRole.fileProviderExtension == "fusekit.file-provider-extension.v1"
    )
  }
}
