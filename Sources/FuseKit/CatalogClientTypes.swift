import Foundation

/// CatalogClientError reports a typed catalog contract failure.
public enum CatalogClientError: Error, Equatable, Sendable {
  case response(CatalogErrorCode, String)
  case missingObject
  case missingMutationIdentifier
  case mutationIdentityMismatch
  case invalidGeneration
}

/// CatalogTenant binds a transport tenant to one immutable catalog generation.
public struct CatalogTenant: Hashable, Sendable {
  public let identifier: CatalogTenantID
  public let generation: UInt64

  public init(identifier: CatalogTenantID, generation: UInt64) throws {
    guard generation > 0 else { throw CatalogClientError.invalidGeneration }
    self.identifier = identifier
    self.generation = generation
  }
}

/// CatalogContentDownload streams exact object bytes and verifies terminal metadata.
public struct CatalogContentDownload: Sendable {
  private let nextOperation: @Sendable () async throws -> Data?
  private let terminal: @Sendable () async throws -> CatalogObject
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> Data?,
    terminal: @escaping @Sendable () async throws -> CatalogObject,
    cancel: @escaping @Sendable () async -> Void
  ) {
    nextOperation = next
    self.terminal = terminal
    cancelOperation = cancel
  }

  public func next() async throws -> Data? {
    try await nextOperation()
  }

  public func response() async throws -> CatalogObject {
    try await terminal()
  }

  public func cancel() async {
    await cancelOperation()
  }
}
