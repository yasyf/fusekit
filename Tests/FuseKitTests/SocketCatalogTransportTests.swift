import Darwin
import Foundation
@testable import FuseKit
import Testing

@Suite("Socket catalog transport")
struct SocketCatalogTransportTests {
  @Test func constructionIsLazyAndFailedConnectionsCanBeRetried() async throws {
    let directory = URL(fileURLWithPath: "/tmp/fkt-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let transport = SocketCatalogTransport(
      socketPath: directory.appendingPathComponent("catalog.sock").path,
      configuration: .init()
    )

    for _ in 0 ..< 2 {
      await #expect(throws: (any Error).self) {
        _ = try await transport.unary(
          operation: .catalogHead,
          tenant: "acct-18",
          payload: Data()
        )
      }
    }
    await transport.activationNotifications().cancel()
  }
}
