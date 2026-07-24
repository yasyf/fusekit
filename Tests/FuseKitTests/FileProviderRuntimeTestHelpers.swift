import CryptoKit
import FileProvider
import Foundation

@testable import FuseKit

extension FileProviderRuntimeTests {
  func makeRuntime(
    rootID: CatalogObjectID,
    transport: MutationTransport,
    accessMode: CatalogTenantAccessMode = .readWrite,
    disposition: CatalogMutationDisposition = .namespace
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
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: FixedMutationDispositionPolicy(disposition),
      materializedSetSource: nil
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
    let symlinkHash = SHA256.hash(data: Data(linkTarget.utf8)).map { String(format: "%02x", $0) }
      .joined()
    return try CatalogObject(
      id: id,
      parentID: parentID,
      revision: 5,
      metadataRevision: 5,
      contentRevision: kind == .directory ? 0 : contentRevision,
      name: name,
      kind: kind,
      mode: mode ?? (kind == .directory ? 0o755 : (kind == .symlink ? 0o777 : 0o644)),
      size: size
        ?? (kind == .directory ? 0 : (kind == .symlink ? UInt64(linkTarget.utf8.count) : 11)),
      hash: kind == .directory ? "" : (kind == .symlink ? symlinkHash : hash),
      linkTarget: linkTarget,
      desired: 5,
      observed: 5,
      verified: 5,
      applied: 5,
      tombstone: false
    )
  }

  func privateResult(
    objectID: CatalogObjectID,
    parentID: CatalogObjectID,
    name: String,
    creator: String = "0000000000000006222222222222222222222222222222222222222222222222"
  ) throws -> CatalogPrivateMutationResult {
    try CatalogPrivateMutationResult(
      creator: try CatalogMutationID(creator),
      objectID: objectID,
      parentID: parentID,
      name: name,
      kind: .file,
      mode: 0o644,
      contentRevision: 1,
      size: 11,
      hash: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
      linkTarget: "",
      createdAgainstHead: 5
    )
  }
}

struct FixedMutationDispositionPolicy: CatalogFileProviderMutationDispositionPolicy {
  let value: CatalogMutationDisposition

  init(_ value: CatalogMutationDisposition) {
    self.value = value
  }

  func disposition(
    for _: any NSFileProviderItem,
    fields _: NSFileProviderItemFields
  ) throws -> CatalogMutationDisposition {
    value
  }
}
