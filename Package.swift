// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "fusekit",
  platforms: [.macOS(.v15)],
  products: [
    .library(name: "FuseKit", targets: ["FuseKit"]),
  ],
  dependencies: [
    .package(
      url: "https://github.com/yasyf/daemonkit.git",
      revision: "4c7cff25f15b9a7744014f43cc1b209bd2c3e16a"
    ),
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
