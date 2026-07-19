import CryptoKit
import FileProvider
import Foundation
@testable import FuseKit
import Testing

@Suite("File Provider mutation identity")
struct FileProviderRuntimeTests {
  @Test
  func createUsesOneStreamAndAcceptsCatalogAssignedOpaqueID() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let assignedID = try CatalogObjectID("10000000000000000000000000000001")
    let created = try object(
      id: assignedID,
      parentID: rootID,
      name: "settings.json",
      contentRevision: 1
    )
    let transport = MutationTransport(source: created, target: created)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let template = CatalogFileProviderItem(object: created, rootID: rootID)
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("new content".utf8).write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    let result = try await runtime.create(template: template, contents: url)

    let mutations = await transport.mutations()
    #expect(mutations.count == 1)
    #expect(mutations[0].request.kind == .create)
    #expect(mutations[0].request.objectID == nil)
    #expect(mutations[0].request.hasContent)
    #expect(mutations[0].request.contentRevision == 1)
    #expect(mutations[0].content == Data("new content".utf8))
    #expect(result.itemIdentifier.rawValue == assignedID.rawValue)
  }

  @Test
  func symlinkCreateCarriesTargetWithoutBodyOrWritingCapability() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let linkID = try CatalogObjectID("10000000000000000000000000000001")
    let link = try object(
      id: linkID,
      parentID: rootID,
      name: "current",
      contentRevision: 1,
      kind: .symlink,
      linkTarget: "../settings.json"
    )
    let transport = MutationTransport(source: link, target: link)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let template = CatalogFileProviderItem(object: link, rootID: rootID)

    let result = try await runtime.create(template: template, contents: nil)

    let mutation = try #require(await transport.mutations().first)
    #expect(mutation.request.objectKind == .symlink)
    #expect(mutation.request.linkTarget == "../settings.json")
    #expect(!mutation.request.hasContent)
    #expect(mutation.content.isEmpty)
    #expect(result.contentType == .symbolicLink)
    #expect(result.symlinkTargetPath == "../settings.json")
    #expect(result.documentSize == nil)
    #expect(!result.capabilities.contains(.allowsWriting))
  }

  @Test
  func atomicReplaceIsOneStreamedMutationAndPreservesTemporaryID() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let destinationID = try CatalogObjectID("00000000000000000000000000000002")
    let temporaryID = try CatalogObjectID("10000000000000000000000000000001")
    let replacedID = try CatalogObjectID("20000000000000000000000000000001")
    let source = try object(
      id: temporaryID,
      parentID: rootID,
      name: ".settings.json.tmp",
      contentRevision: 3,
      mode: 0o755
    )
    let replaced = try object(
      id: replacedID,
      parentID: destinationID,
      name: "settings.json",
      contentRevision: 8
    )
    let proposed = try CatalogFileProviderItem(
      object: object(
        id: temporaryID,
        parentID: destinationID,
        name: "settings.json",
        contentRevision: 3
      ),
      rootID: rootID
    )
    let transport = MutationTransport(source: source, target: replaced)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("replacement".utf8).write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    let result = try await runtime.modify(
      item: proposed,
      baseVersion: CatalogFileProviderItem(object: source, rootID: rootID).itemVersion,
      changedFields: [.filename, .parentItemIdentifier, .contents],
      contents: url
    )

    await expectAtomicReplacement(
      transport.mutations(),
      result: result,
      temporaryID: temporaryID,
      replacedID: replacedID
    )
  }

  @Test
  func rootUsesRootContainerIdentityAndCannotBeRenamedOrDeleted() throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let root = try CatalogFileProviderItem(
      object: object(
        id: rootID,
        parentID: rootID,
        name: "Account",
        contentRevision: 1,
        kind: .directory
      ),
      rootID: rootID
    )

    #expect(root.itemIdentifier == .rootContainer)
    #expect(root.parentItemIdentifier == .rootContainer)
    #expect(
      root.capabilities
        == [.allowsReading, .allowsContentEnumerating, .allowsAddingSubItems]
    )
  }

  @Test
  func immutableObjectKindIsRejectedBeforeMutation() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let sourceID = try CatalogObjectID("10000000000000000000000000000001")
    let source = try object(
      id: sourceID,
      parentID: rootID,
      name: "settings.json",
      contentRevision: 3
    )
    let proposedDirectory = try CatalogFileProviderItem(
      object: object(
        id: sourceID,
        parentID: rootID,
        name: "settings.json",
        contentRevision: 3,
        kind: .directory
      ),
      rootID: rootID
    )
    let transport = MutationTransport(source: source, target: source)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)

    await #expect(throws: NSFileProviderError.self) {
      _ = try await runtime.modify(
        item: proposedDirectory,
        baseVersion: CatalogFileProviderItem(object: source, rootID: rootID).itemVersion,
        changedFields: .filename,
        contents: nil
      )
    }
    #expect(await transport.mutations().isEmpty)
  }

  private func expectAtomicReplacement(
    _ mutations: [MutationTransport.Mutation],
    result: CatalogFileProviderItem,
    temporaryID: CatalogObjectID,
    replacedID: CatalogObjectID
  ) {
    #expect(mutations.count == 1)
    #expect(mutations[0].request.kind == .replace)
    #expect(mutations[0].request.objectID == temporaryID)
    #expect(mutations[0].request.targetID == replacedID)
    #expect(mutations[0].request.hasContent)
    #expect(mutations[0].request.mode == 0o755)
    #expect(mutations[0].content == Data("replacement".utf8))
    #expect(result.itemIdentifier.rawValue == temporaryID.rawValue)
  }
}

extension FileProviderRuntimeTests {
  @Test
  func multiMegabyteUploadIsPulledInBoundedChunks() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let assignedID = try CatalogObjectID("10000000000000000000000000000001")
    let created = try object(
      id: assignedID,
      parentID: rootID,
      name: "large.bin",
      contentRevision: 1
    )
    let transport = MutationTransport(source: created, target: created)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let template = CatalogFileProviderItem(object: created, rootID: rootID)
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    let payload = Data(repeating: 0x5A, count: 8 * 1024 * 1024 + 17)
    try payload.write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    _ = try await runtime.create(template: template, contents: url)

    let mutation = try #require(await transport.mutations().first)
    #expect(mutation.content == payload)
    #expect(mutation.chunkSizes.count == 9)
    #expect(mutation.chunkSizes.max() == 1024 * 1024)
  }

  @Test
  func onlyImplementedFieldsAreReportedAppliedAndAllOptionsAreRejected() throws {
    let requested: NSFileProviderItemFields = [
      .filename, .parentItemIdentifier, .contents, .creationDate,
    ]
    let remaining = CatalogFileProviderOperationPolicy.remaining(requested)
    #expect(!remaining.contains(.filename))
    #expect(!remaining.contains(.parentItemIdentifier))
    #expect(!remaining.contains(.contents))
    #expect(remaining.contains(.creationDate))

    #expect(throws: CatalogFileProviderOperationError.unsupportedCreateOptions) {
      try CatalogFileProviderOperationPolicy.validate(
        NSFileProviderCreateItemOptions(rawValue: 1 << 30)
      )
    }
    #expect(throws: CatalogFileProviderOperationError.unsupportedModifyOptions) {
      try CatalogFileProviderOperationPolicy.validate(
        NSFileProviderModifyItemOptions(rawValue: 1 << 30)
      )
    }
    #expect(throws: CatalogFileProviderOperationError.unsupportedDeleteOptions) {
      try CatalogFileProviderOperationPolicy.validate(
        NSFileProviderDeleteItemOptions(rawValue: 1 << 30)
      )
    }
  }

  @Test
  func slowDownloadFailureAbortsPullsAndSettlesUpstream() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let fileID = try CatalogObjectID("10000000000000000000000000000001")
    let file = try object(
      id: fileID,
      parentID: rootID,
      name: "large.bin",
      contentRevision: 2
    )
    let source = DownloadSource()
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID
      ),
      client: CatalogClient(transport: DownloadTransport(object: file, source: source))
    )

    await #expect(throws: DownloadTestError.interrupted) {
      _ = try await runtime.fetchContents(
        for: NSFileProviderItemIdentifier(fileID.rawValue),
        requestedVersion: nil
      )
    }
    #expect(await source.pullCount() == 3)
    #expect(await source.wasCanceled())
  }

  @Test
  func downloadedBytesMustMatchCatalogHash() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let fileID = try CatalogObjectID("10000000000000000000000000000001")
    let file = try object(
      id: fileID,
      parentID: rootID,
      name: "verified.txt",
      contentRevision: 2,
      size: 11,
      hash: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )
    let source = DownloadSource(
      chunks: [Data("hello ".utf8), Data("world".utf8)],
      failureAt: nil
    )
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID
      ),
      client: CatalogClient(transport: DownloadTransport(object: file, source: source))
    )

    let (url, _) = try await runtime.fetchContents(
      for: NSFileProviderItemIdentifier(fileID.rawValue),
      requestedVersion: nil
    )
    defer { try? FileManager.default.removeItem(at: url) }
    #expect(try Data(contentsOf: url) == Data("hello world".utf8))
    #expect(await !(source.wasCanceled()))
  }

  private func makeRuntime(
    rootID: CatalogObjectID,
    transport: MutationTransport
  ) throws -> CatalogFileProviderRuntime {
    try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(
          identifier: CatalogTenantID("tenant-1"),
          generation: 4
        ),
        rootID: rootID
      ),
      client: CatalogClient(transport: transport)
    )
  }

  private func object(
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
