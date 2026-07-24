import Foundation
@testable import FuseKit
import Testing

@Suite("Broker child mode")
struct BrokerChildModeTests {
  @Test
  func brokerConfigurationPinsExactRuntimeReadiness() throws {
    let endpoint = try CatalogAppGroupEndpoint(
      identifier: "group.example.product",
      socketLeaf: "catalog.sock"
    )
    let configuration = CatalogBroker.Configuration(
      appGroupEndpoint: endpoint,
      daemonSocketPath: "/tmp/daemon.sock",
      expectedRuntimeBuild: "product.v1",
      noProgressTimeout: 5
    )

    #expect(configuration.appGroupEndpoint == endpoint)
    #expect(configuration.daemonSocketPath == "/tmp/daemon.sock")
    #expect(configuration.expectedRuntimeBuild == "product.v1")
    #expect(configuration.noProgressTimeout == 5)
  }

  @Test
  func parsesOnlyExactFixedAppArguments() throws {
    let path = "/Users/example/Library/Application Support/Product/fusekit.sock"
    let child = try CatalogBrokerChildMode.parse(arguments: [
      "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
      "--fusekit-broker-child",
      "--fusekit-daemon-socket",
      path,
    ])
    #expect(child == CatalogBrokerChildMode(daemonSocketPath: path))
  }

  @Test
  func normalApplicationStartupIsNotClaimed() throws {
    let child = try CatalogBrokerChildMode.parse(arguments: [
      "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
    ])
    #expect(child == nil)
  }

  @Test(
    arguments: [
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-daemon-socket",
        "/tmp/fusekit.sock",
        "--fusekit-broker-child",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "relative.sock",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "/tmp/../tmp/fusekit.sock",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "/tmp/fusekit.sock",
        "--unexpected",
      ],
    ]
  )
  func rejectsPartialReorderedNoncanonicalOrExtendedArguments(_ arguments: [String]) {
    #expect(throws: CatalogBrokerChildError.invalidArguments) {
      try CatalogBrokerChildMode.parse(arguments: arguments)
    }
  }
}
