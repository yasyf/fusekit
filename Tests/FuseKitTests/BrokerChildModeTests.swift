import Foundation
import Testing
@testable import FuseKit

@Suite("Broker child mode")
struct BrokerChildModeTests {
  @Test
  func parsesOnlyExactFixedAppArguments() throws {
    let path = "/Users/example/Library/Application Support/Product/fusekit.sock"
    let child = try CatalogBrokerChildMode.parse(arguments: [
      "/Applications/Product.app/Contents/MacOS/Product",
      "--fusekit-broker-child",
      "--fusekit-daemon-socket",
      path,
    ])
    #expect(child == CatalogBrokerChildMode(daemonSocketPath: path))
  }

  @Test
  func normalApplicationStartupIsNotClaimed() throws {
    let child = try CatalogBrokerChildMode.parse(arguments: [
      "/Applications/Product.app/Contents/MacOS/Product"
    ])
    #expect(child == nil)
  }

  @Test(
    arguments: [
      [
        "/Applications/Product.app/Contents/MacOS/Product",
        "--fusekit-broker-child",
      ],
      [
        "/Applications/Product.app/Contents/MacOS/Product",
        "--fusekit-daemon-socket",
        "/tmp/fusekit.sock",
        "--fusekit-broker-child",
      ],
      [
        "/Applications/Product.app/Contents/MacOS/Product",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "relative.sock",
      ],
      [
        "/Applications/Product.app/Contents/MacOS/Product",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "/tmp/../tmp/fusekit.sock",
      ],
      [
        "/Applications/Product.app/Contents/MacOS/Product",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "/tmp/fusekit.sock",
        "--unexpected",
      ],
    ])
  func rejectsPartialReorderedNoncanonicalOrExtendedArguments(_ arguments: [String]) {
    #expect(throws: CatalogBrokerChildError.invalidArguments) {
      try CatalogBrokerChildMode.parse(arguments: arguments)
    }
  }
}
