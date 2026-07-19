@preconcurrency import FileProvider

/// CatalogFileProviderBindingError rejects invalid runtime policy.
public enum CatalogFileProviderBindingError: Error, Equatable, Sendable {
  case invalidPageSize
}

/// CatalogFileProviderBinding fixes one File Provider domain to one catalog tenant.
public struct CatalogFileProviderBinding: Sendable {
  public static let maximumPageSize = CatalogProtocol.maxPageSize

  public let domainID: CatalogDomainID
  public let tenant: CatalogTenant
  public let rootID: CatalogObjectID
  public let accessMode: CatalogTenantAccessMode
  public let pageSize: UInt32

  public init(
    domainID: CatalogDomainID,
    tenant: CatalogTenant,
    rootID: CatalogObjectID,
    accessMode: CatalogTenantAccessMode,
    pageSize: UInt32 = 256
  ) throws {
    guard (1 ... Self.maximumPageSize).contains(pageSize) else {
      throw CatalogFileProviderBindingError.invalidPageSize
    }
    self.domainID = domainID
    self.tenant = tenant
    self.rootID = rootID
    self.accessMode = accessMode
    self.pageSize = pageSize
  }

  /// init(domain:) reconstructs the exact registered binding without daemon I/O.
  public init(domain: NSFileProviderDomain, pageSize: UInt32 = 256) throws {
    let metadata = try CatalogDomainMetadata(domain: domain)
    try self.init(
      domainID: metadata.domainID,
      tenant: CatalogTenant(identifier: metadata.tenantID, generation: metadata.generation),
      rootID: metadata.rootID,
      accessMode: metadata.accessMode,
      pageSize: pageSize
    )
  }
}
