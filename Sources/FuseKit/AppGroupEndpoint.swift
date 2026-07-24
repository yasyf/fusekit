import DaemonKit

/// CatalogAppGroupEndpoint is the one signed broker-extension socket contract.
public struct CatalogAppGroupEndpoint: Equatable, Sendable {
  public let identifier: String
  public let socketLeaf: String

  let container: AppGroupContainer
  let leaf: AppGroupContainer.SocketLeaf

  public init(identifier: String, socketLeaf: String) throws {
    container = try AppGroupContainer(identifier: identifier)
    leaf = try AppGroupContainer.SocketLeaf(socketLeaf)
    self.identifier = identifier
    self.socketLeaf = socketLeaf
  }

  /// socketPath resolves the endpoint through the caller's signed App Group entitlement.
  public func socketPath() throws -> String {
    try container.socketPath(leaf: leaf)
  }

  public static func == (left: Self, right: Self) -> Bool {
    left.identifier == right.identifier && left.socketLeaf == right.socketLeaf
  }
}
