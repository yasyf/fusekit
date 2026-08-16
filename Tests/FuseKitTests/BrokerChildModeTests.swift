import DaemonKit
import Foundation
@testable import FuseKit
import Testing

@Suite("Broker child mode")
struct BrokerChildModeTests {
  @Test
  func brokerChildModeWithoutASpawnChannelFailsClosed() async throws {
    let endpoint = try CatalogAppGroupEndpoint(
      identifier: "group.example.product",
      socketLeaf: "catalog.sock"
    )
    await #expect(throws: SpawnedChannelError.notSpawned) {
      _ = try await CatalogBroker.runChildIfRequested(
        arguments: [
          "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
          "--fusekit-broker-child",
        ],
        configuration: .init(appGroupEndpoint: endpoint)
      )
    }
  }

  @Test
  func brokerConfigurationPinsTheSignedEndpoint() throws {
    let endpoint = try CatalogAppGroupEndpoint(
      identifier: "group.example.product",
      socketLeaf: "catalog.sock"
    )
    let configuration = CatalogBroker.Configuration(appGroupEndpoint: endpoint)

    #expect(configuration.appGroupEndpoint == endpoint)
  }

  @Test
  func recognizesOnlyExactFixedAppArguments() throws {
    let requested = try CatalogBrokerChildMode.requested(arguments: [
      "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
      "--fusekit-broker-child",
    ])
    #expect(requested)
  }

  @Test
  func normalApplicationStartupIsNotClaimed() throws {
    let requested = try CatalogBrokerChildMode.requested(arguments: [
      "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
    ])
    #expect(!requested)
  }

  @Test(
    arguments: [
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
        "--fusekit-daemon-socket",
        "/tmp/fusekit.sock",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-daemon-socket",
        "--fusekit-broker-child",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--fusekit-broker-child",
        "--unexpected",
      ],
      [
        "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
        "--unexpected",
        "--fusekit-broker-child",
      ],
    ]
  )
  func rejectsExtendedOrSocketCarryingArguments(_ arguments: [String]) {
    #expect(throws: CatalogBrokerChildError.invalidArguments) {
      try CatalogBrokerChildMode.requested(arguments: arguments)
    }
  }
}
