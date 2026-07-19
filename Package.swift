// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "fusekit",
  platforms: [.macOS(.v15)],
  products: [
    .library(name: "FuseKit", targets: ["FuseKit"]),
  ],
  dependencies: [
    .package(url: "https://github.com/yasyf/daemonkit.git", revision: "f090a23cf11fa99eb3de55cc656f8884a8c6acb6"),
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
