// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "fusekit",
  platforms: [.macOS(.v15)],
  products: [
    .library(name: "FuseKit", targets: ["FuseKit"]),
  ],
  dependencies: [
    .package(url: "https://github.com/yasyf/daemonkit.git", revision: "3ba42c707738b419d13e0aabc1bb801a77024b52"),
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
