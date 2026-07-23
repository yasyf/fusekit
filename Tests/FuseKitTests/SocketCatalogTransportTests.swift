import DaemonKit
import Darwin
import Foundation
@testable import FuseKit
import Testing

@Suite("Socket catalog transport")
struct SocketCatalogTransportTests {
  @Test func constructionIsLazyAndAFailedConnectionCanBeRetried() async throws {
    let directory = URL(fileURLWithPath: "/tmp/fkt-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let path = directory.appendingPathComponent("catalog.sock").path
    let transport = SocketCatalogTransport(
      socketPath: path,
      configuration: .init(),
      trust: .sameEffectiveUser
    )

    await #expect(throws: (any Error).self) {
      _ = try await transport.unary(operation: .catalogHead, tenant: "acct-18", payload: Data())
    }

    let server = SocketServer(
      path: path,
      wireBuild: FuseKitTransportProtocol.wireBuild,
      trust: .sameEffectiveUser
    ) { request in
      .terminal(SocketTerminal(payload: request.payload))
    }
    try await server.start()
    do {
      let payload = Data(#"{"ready":true}"#.utf8)
      let response = try await transport.unary(
        operation: .catalogHead,
        tenant: "acct-18",
        payload: payload
      )
      #expect(response == payload)
      await transport.convergenceNotifications().cancel()
      await server.stop()
    } catch {
      await transport.convergenceNotifications().cancel()
      await server.stop()
      throw error
    }
  }
}
