import CryptoKit
import Foundation
@testable import FuseKit

extension FileProviderRuntimeTests {
  func makeRuntime(
    rootID: CatalogObjectID,
    transport: MutationTransport,
    accessMode: CatalogTenantAccessMode = .readWrite
  ) throws -> CatalogFileProviderRuntime {
    try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(
          identifier: CatalogTenantID("tenant-1"),
          generation: 4
        ),
        rootID: rootID,
        accessMode: accessMode
      ),
      client: CatalogClient(transport: transport)
    )
  }

  func object(
    id: CatalogObjectID,
    parentID: CatalogObjectID,
    name: String,
    contentRevision: UInt64,
    kind: CatalogObjectKind = .file,
    mode: UInt32? = nil,
    size: UInt64? = nil,
    hash: String = "hash",
    linkTarget: String = ""
  ) throws -> CatalogObject {
    let symlinkHash = SHA256.hash(data: Data(linkTarget.utf8)).map { String(format: "%02x", $0) }.joined()
    return try CatalogObject(
      id: id,
      parentID: parentID,
      revision: 5,
      metadataRevision: 5,
      contentRevision: kind == .directory ? 0 : contentRevision,
      name: name,
      kind: kind,
      mode: mode ?? (kind == .directory ? 0o755 : (kind == .symlink ? 0o777 : 0o644)),
      size: size ?? (kind == .directory ? 0 : (kind == .symlink ? UInt64(linkTarget.utf8.count) : 11)),
      hash: kind == .directory ? "" : (kind == .symlink ? symlinkHash : hash),
      linkTarget: linkTarget,
      desired: 5,
      observed: 5,
      verified: 5,
      applied: 5,
      tombstone: false
    )
  }
}
