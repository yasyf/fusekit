// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "fusekit",
  platforms: [.macOS(.v15)],
  products: [
    .library(name: "FuseKit", targets: ["FuseKit"]),
  ],
  dependencies: [
    .package(url: "https://github.com/yasyf/daemonkit.git", revision: "173abb1e83d4dcb50aa643f5647c6d4b277c752c"),
  ],
  targets: [
    .target(
      name: "FuseKit",
      dependencies: [.product(name: "DaemonKit", package: "daemonkit")]
    ),
    .testTarget(
      name: "FuseKitTests",
      dependencies: ["FuseKit", .product(name: "DaemonKit", package: "daemonkit")]
    ),
  ]
)
