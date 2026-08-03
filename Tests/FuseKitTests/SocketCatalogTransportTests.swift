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

  @Test func requestEnvelopeEmbedsRawPayloadUnderTheRoutingTenant() throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let request = try CatalogReadRequest(
      handle: CatalogHandleID("0123456789abcdef0123456789abcdef"),
      offset: 9_007_199_254_740_993,
      limit: 1024
    )
    let payload = try encoder.encode(request)

    let framed = try CatalogRequestEnvelope.encode(tenant: "acct-18", payload: payload)
    let decoded = try JSONDecoder().decode(RoutingEnvelope.self, from: framed)
    #expect(decoded.tenant == "acct-18")
    #expect(decoded.payload.offset == 9_007_199_254_740_993)
    #expect(decoded.payload.limit == 1024)

    let sessionScoped = try CatalogRequestEnvelope.encode(tenant: "", payload: payload)
    let sessionDecoded = try JSONDecoder().decode(RoutingEnvelope.self, from: sessionScoped)
    #expect(sessionDecoded.tenant == "")
    #expect(sessionDecoded.payload.offset == 9_007_199_254_740_993)

    let escaped = try CatalogRequestEnvelope.encode(tenant: "a\"b", payload: payload)
    #expect(try JSONDecoder().decode(RoutingEnvelope.self, from: escaped).tenant == "a\"b")
  }

  private struct RoutingEnvelope: Decodable {
    let tenant: String
    let payload: CatalogReadRequest
  }
}
