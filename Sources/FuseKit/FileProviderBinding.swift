@preconcurrency import FileProvider

/// CatalogFileProviderBinding fixes one File Provider domain to one catalog tenant.
public struct CatalogFileProviderBinding: Sendable {
  public let domainID: CatalogDomainID
  public let tenant: CatalogTenant
  public let rootID: CatalogObjectID
  public let pageSize: UInt32

  public init(
    domainID: CatalogDomainID,
    tenant: CatalogTenant,
    rootID: CatalogObjectID,
    pageSize: UInt32 = 256
  ) {
    self.domainID = domainID
    self.tenant = tenant
    self.rootID = rootID
    self.pageSize = pageSize
  }

  /// init(domain:) reconstructs the exact registered binding without daemon I/O.
  public init(domain: NSFileProviderDomain, pageSize: UInt32 = 256) throws {
    let metadata = try CatalogDomainMetadata(domain: domain)
    try self.init(
      domainID: metadata.domainID,
      tenant: CatalogTenant(identifier: metadata.tenantID, generation: metadata.generation),
      rootID: metadata.rootID,
      pageSize: pageSize
    )
  }
}
