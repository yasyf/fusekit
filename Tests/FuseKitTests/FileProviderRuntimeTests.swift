import CryptoKit
import FileProvider
import Foundation
import Testing

@testable import FuseKit

@Suite("File Provider mutation identity")
struct FileProviderRuntimeTests {
  @Test
  func criticalFetchAcknowledgementRequiresExactReadDigest() throws {
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogAckCriticalFetchRequest(
        generation: 4,
        objectID: CatalogObjectID("10000000000000000000000000000001"),
        objectRevision: 5,
        contentRevision: 2,
        size: 11,
        hash: String(repeating: "a", count: 64),
        readHash: String(repeating: "b", count: 64),
        leaseID: "lease-1",
        resolutionDigest: String(repeating: "2", count: 64),
        readChallenge: String(repeating: "5", count: 64)
      )
    }
  }

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
    let template = CatalogFileProviderItem(
      object: created,
      rootID: rootID,
      accessMode: .readWrite
    )
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("new content".utf8).write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    let result = try await runtime.create(template: template, fields: [.contents], contents: url)

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
  func privateCreateUsesExplicitPolicyAndReturnsTerminalItemWithoutLookup() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let assignedID = try CatalogObjectID("10000000000000000000000000000001")
    let created = try object(
      id: assignedID,
      parentID: rootID,
      name: ".settings.json.tmp",
      contentRevision: 1
    )
    let privateResult = try privateResult(
      objectID: assignedID,
      parentID: rootID,
      name: ".settings.json.tmp"
    )
    let transport = MutationTransport(
      source: created,
      target: created,
      privateResult: privateResult
    )
    let runtime = try makeRuntime(
      rootID: rootID,
      transport: transport,
      disposition: .privateStaging
    )
    let template = CatalogFileProviderItem(
      object: created,
      rootID: rootID,
      accessMode: .readWrite
    )
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("new content".utf8).write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    let result = try await runtime.create(
      template: template,
      fields: [.filename, .contents],
      contents: url
    )

    let mutation = try #require(await transport.mutations().first)
    #expect(mutation.request.disposition == .privateStaging)
    #expect(mutation.request.privateCreator == nil)
    #expect(result.itemIdentifier.rawValue == assignedID.rawValue)
    let counts = await transport.lookupCounts()
    #expect(counts.public == 0)
    #expect(counts.private == 0)
  }

  @Test
  func privateDeleteFallsBackOnlyAfterPublicNotFoundAndCarriesCreator() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let privateID = try CatalogObjectID("10000000000000000000000000000001")
    let source = try object(
      id: privateID,
      parentID: rootID,
      name: ".settings.json.tmp",
      contentRevision: 1
    )
    let privateResult = try privateResult(
      objectID: privateID,
      parentID: rootID,
      name: ".settings.json.tmp"
    )
    let transport = MutationTransport(source: source, target: nil, privateResult: privateResult)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let privateItem = CatalogFileProviderItem(
      privateResult: privateResult,
      rootID: rootID,
      accessMode: .readWrite
    )

    try await runtime.delete(
      identifier: privateItem.itemIdentifier, baseVersion: privateItem.itemVersion)

    let mutation = try #require(await transport.mutations().first)
    #expect(mutation.request.kind == .delete)
    #expect(mutation.request.disposition == .privateStaging)
    #expect(mutation.request.privateCreator == privateResult.creator)
    let counts = await transport.lookupCounts()
    #expect(counts.public == 1)
    #expect(counts.private == 1)
  }

  @Test
  func privateRenameWithoutTargetUsesExplicitPromotionCapability() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let privateID = try CatalogObjectID("10000000000000000000000000000001")
    let promoted = try object(
      id: privateID,
      parentID: rootID,
      name: "settings.json",
      contentRevision: 1
    )
    let privateResult = try privateResult(
      objectID: privateID,
      parentID: rootID,
      name: ".settings.json.tmp"
    )
    let transport = MutationTransport(source: promoted, target: nil, privateResult: privateResult)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let proposed = CatalogFileProviderItem(
      object: promoted,
      rootID: rootID,
      accessMode: .readWrite
    )
    let base = CatalogFileProviderItem(
      privateResult: privateResult,
      rootID: rootID,
      accessMode: .readWrite
    ).itemVersion

    _ = try await runtime.modify(
      item: proposed,
      baseVersion: base,
      changedFields: [.filename],
      contents: nil
    )

    let mutation = try #require(await transport.mutations().first)
    #expect(mutation.request.kind == .promote)
    #expect(mutation.request.disposition == .namespace)
    #expect(mutation.request.privateCreator == privateResult.creator)
    #expect(mutation.request.targetID == nil)
  }

  @Test
  func privateContentOnlyModifyDoesNotAccidentallyPublish() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let privateID = try CatalogObjectID("10000000000000000000000000000001")
    let source = try object(
      id: privateID,
      parentID: rootID,
      name: ".settings.json.tmp",
      contentRevision: 1
    )
    let privateResult = try privateResult(
      objectID: privateID,
      parentID: rootID,
      name: ".settings.json.tmp"
    )
    let transport = MutationTransport(source: source, target: nil, privateResult: privateResult)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let item = CatalogFileProviderItem(
      privateResult: privateResult,
      rootID: rootID,
      accessMode: .readWrite
    )

    await #expect(throws: NSFileProviderError.self) {
      _ = try await runtime.modify(
        item: item,
        baseVersion: item.itemVersion,
        changedFields: [.contents],
        contents: nil
      )
    }
    #expect(await transport.mutations().isEmpty)
  }

  @Test
  func privateFetchUsesCapabilityScopedLookupAndOpen() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let privateID = try CatalogObjectID("10000000000000000000000000000001")
    let source = try object(
      id: privateID,
      parentID: rootID,
      name: ".settings.json.tmp",
      contentRevision: 1
    )
    let privateResult = try privateResult(
      objectID: privateID,
      parentID: rootID,
      name: ".settings.json.tmp"
    )
    let transport = MutationTransport(
      source: source,
      target: nil,
      privateResult: privateResult,
      privateContent: Data("hello world".utf8)
    )
    let runtime = try makeRuntime(rootID: rootID, transport: transport)

    let (url, item) = try await runtime.fetchContents(
      for: NSFileProviderItemIdentifier(privateID.rawValue),
      requestedVersion: nil
    )
    defer { try? FileManager.default.removeItem(at: url) }

    #expect(try Data(contentsOf: url) == Data("hello world".utf8))
    #expect(item.itemIdentifier.rawValue == privateID.rawValue)
    let counts = await transport.lookupCounts()
    #expect(counts.public == 1)
    #expect(counts.private == 1)
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
    let template = CatalogFileProviderItem(
      object: link,
      rootID: rootID,
      accessMode: .readWrite
    )

    let result = try await runtime.create(template: template, fields: [.filename], contents: nil)

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
      rootID: rootID,
      accessMode: .readWrite
    )
    let transport = MutationTransport(source: source, target: replaced)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("replacement".utf8).write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    let result = try await runtime.modify(
      item: proposed,
      baseVersion: CatalogFileProviderItem(
        object: source,
        rootID: rootID,
        accessMode: .readWrite
      ).itemVersion,
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
      rootID: rootID,
      accessMode: .readWrite
    )

    #expect(root.itemIdentifier == .rootContainer)
    #expect(root.parentItemIdentifier == .rootContainer)
    #expect(
      root.capabilities
        == [.allowsReading, .allowsContentEnumerating, .allowsAddingSubItems]
    )
  }

  @Test
  func readOnlyTenantAdvertisesNoMutationCapabilitiesAndRejectsMutation() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let fileID = try CatalogObjectID("10000000000000000000000000000001")
    let root = try object(
      id: rootID,
      parentID: rootID,
      name: "Account",
      contentRevision: 1,
      kind: .directory
    )
    let file = try object(
      id: fileID,
      parentID: rootID,
      name: "settings.json",
      contentRevision: 1
    )
    let rootItem = CatalogFileProviderItem(
      object: root,
      rootID: rootID,
      accessMode: .readOnly
    )
    let fileItem = CatalogFileProviderItem(
      object: file,
      rootID: rootID,
      accessMode: .readOnly
    )
    let transport = MutationTransport(source: file, target: file)
    let runtime = try makeRuntime(
      rootID: rootID,
      transport: transport,
      accessMode: .readOnly
    )

    #expect(rootItem.capabilities == [.allowsReading, .allowsContentEnumerating])
    #expect(fileItem.capabilities == [.allowsReading])
    await #expect(throws: NSFileProviderError.self) {
      _ = try await runtime.create(template: fileItem, fields: [], contents: nil)
    }
    #expect(await transport.mutations().isEmpty)
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
      rootID: rootID,
      accessMode: .readWrite
    )
    let transport = MutationTransport(source: source, target: source)
    let runtime = try makeRuntime(rootID: rootID, transport: transport)

    await #expect(throws: NSFileProviderError.self) {
      _ = try await runtime.modify(
        item: proposedDirectory,
        baseVersion: CatalogFileProviderItem(
          object: source,
          rootID: rootID,
          accessMode: .readWrite
        ).itemVersion,
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
    let template = CatalogFileProviderItem(
      object: created,
      rootID: rootID,
      accessMode: .readWrite
    )
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    let payload = Data(repeating: 0x5A, count: 8 * 1024 * 1024 + 17)
    try payload.write(to: url)
    defer { try? FileManager.default.removeItem(at: url) }

    _ = try await runtime.create(template: template, fields: [.contents], contents: url)

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
    let transport = DownloadTransport(object: file, source: source)
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID,
        accessMode: .readWrite
      ),
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: FixedMutationDispositionPolicy(.namespace),
      materializedSetSource: nil
    )

    await #expect(throws: DownloadTestError.interrupted) {
      _ = try await runtime.fetchContents(
        for: NSFileProviderItemIdentifier(fileID.rawValue),
        requestedVersion: nil
      )
    }
    #expect(await source.pullCount() == 3)
    #expect(await source.wasCanceled())
    #expect(await transport.criticalFetchAcks().isEmpty)
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
    let context = try criticalFetchContext()
    let transport = DownloadTransport(
      object: file,
      source: source,
      criticalContext: context
    )
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID,
        accessMode: .readWrite
      ),
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: FixedMutationDispositionPolicy(.namespace),
      materializedSetSource: nil
    )

    let (url, _) = try await runtime.fetchContents(
      for: NSFileProviderItemIdentifier(fileID.rawValue),
      requestedVersion: nil
    )
    defer { try? FileManager.default.removeItem(at: url) }
    #expect(try Data(contentsOf: url) == Data("hello world".utf8))
    #expect(await !(source.wasCanceled()))
    let resolutions = await transport.criticalFetchResolves()
    #expect(resolutions.count == 1)
    let resolution = try #require(resolutions.first)
    #expect(resolution.tenant == "tenant-1")
    #expect(resolution.request.generation == 4)
    #expect(resolution.request.objectID == fileID)
    #expect(resolution.request.objectRevision == file.revision)
    #expect(resolution.request.contentRevision == file.contentRevision)
    #expect(resolution.request.size == file.size)
    #expect(resolution.request.hash == file.hash)
    let acknowledgements = await transport.criticalFetchAcks()
    #expect(acknowledgements.count == 1)
    let acknowledgement = try #require(acknowledgements.first)
    #expect(acknowledgement.tenant == "tenant-1")
    #expect(acknowledgement.request.generation == 4)
    #expect(acknowledgement.request.objectID == fileID)
    #expect(acknowledgement.request.objectRevision == file.revision)
    #expect(acknowledgement.request.contentRevision == file.contentRevision)
    #expect(acknowledgement.request.size == file.size)
    #expect(acknowledgement.request.hash == file.hash)
    #expect(acknowledgement.request.readHash == file.hash)
    #expect(acknowledgement.request.leaseID == context.leaseID)
    #expect(acknowledgement.request.resolutionDigest == context.resolutionDigest)
    #expect(acknowledgement.request.readChallenge == context.readChallenge)
  }

  @Test
  func criticalFetchAcknowledgementFailureRejectsCompletedDownload() async throws {
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
    let expected = CatalogTransportError.remote("ack rejected")
    let transport = try DownloadTransport(
      object: file,
      source: source,
      criticalContext: criticalFetchContext(),
      ackError: expected
    )
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID,
        accessMode: .readWrite
      ),
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: FixedMutationDispositionPolicy(.namespace),
      materializedSetSource: nil
    )

    await #expect(throws: expected) {
      _ = try await runtime.fetchContents(
        for: NSFileProviderItemIdentifier(fileID.rawValue),
        requestedVersion: nil
      )
    }
    #expect(await transport.criticalFetchAcks().count == 1)
  }

  @Test
  func successfulNoncriticalDownloadCompletesWithoutAcknowledgement() async throws {
    let rootID = try CatalogObjectID("00000000000000000000000000000001")
    let fileID = try CatalogObjectID("10000000000000000000000000000001")
    let file = try object(
      id: fileID,
      parentID: rootID,
      name: "ordinary.txt",
      contentRevision: 2,
      size: 11,
      hash: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )
    let source = DownloadSource(
      chunks: [Data("hello ".utf8), Data("world".utf8)],
      failureAt: nil
    )
    let transport = DownloadTransport(object: file, source: source)
    let runtime = try CatalogFileProviderRuntime(
      binding: CatalogFileProviderBinding(
        domainID: runtimeDomainID(),
        tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
        rootID: rootID,
        accessMode: .readWrite
      ),
      client: CatalogClient(transport: transport),
      mutationDispositionPolicy: FixedMutationDispositionPolicy(.namespace),
      materializedSetSource: nil
    )

    let (url, _) = try await runtime.fetchContents(
      for: NSFileProviderItemIdentifier(fileID.rawValue),
      requestedVersion: nil
    )
    defer { try? FileManager.default.removeItem(at: url) }
    #expect(await transport.criticalFetchResolves().count == 1)
    #expect(await transport.criticalFetchAcks().isEmpty)
  }
}
