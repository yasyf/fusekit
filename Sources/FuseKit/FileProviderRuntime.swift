import CryptoKit
import FileProvider
import Foundation
import UniformTypeIdentifiers

/// CatalogFileProviderRuntime owns generic lookup, fetch, enumeration, and mutation behavior.
public final class CatalogFileProviderRuntime: Sendable {
  public let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private let mutationDispositionPolicy: any CatalogFileProviderMutationDispositionPolicy
  private let activation: CatalogActivationInbox
  private let bindingGate: CatalogBindingGate
  private let materialization: CatalogMaterializationCoordinator?
  private let notificationTask: Task<Void, Never>

  public convenience init(
    domain: NSFileProviderDomain,
    binding: CatalogFileProviderBinding,
    client: CatalogClient,
    mutationDispositionPolicy: any CatalogFileProviderMutationDispositionPolicy
  ) throws {
    try self.init(
      binding: binding,
      client: client,
      mutationDispositionPolicy: mutationDispositionPolicy,
      materializedSetSource: NativeCatalogMaterializedSetSource(domain: domain)
    )
  }

  init(
    binding: CatalogFileProviderBinding,
    client: CatalogClient,
    mutationDispositionPolicy: any CatalogFileProviderMutationDispositionPolicy,
    materializedSetSource: (any CatalogMaterializedSetSource)?
  ) {
    self.binding = binding
    self.client = client
    self.mutationDispositionPolicy = mutationDispositionPolicy
    let bindingGate = CatalogBindingGate(binding: binding, client: client)
    self.bindingGate = bindingGate
    let activation = CatalogActivationInbox(binding: binding, client: client)
    self.activation = activation
    let materialization = materializedSetSource.map {
      CatalogMaterializationCoordinator(
        binding: binding,
        client: client,
        bindingGate: bindingGate,
        source: $0
      )
    }
    self.materialization = materialization
    notificationTask = Task {
      let notifications = client.activationNotifications()
      do {
        try await bindingGate.bind()
        while let notification = try await notifications.next() {
          try await activation.receive(notification)
        }
      } catch is CancellationError {
        await notifications.cancel()
      } catch {
        await notifications.cancel()
        await activation.fail(error)
      }
    }
    materialization?.markDirty()
  }

  deinit {
    notificationTask.cancel()
  }

  /// invalidate stops the persistent activation event consumer.
  public func invalidate() {
    notificationTask.cancel()
    materialization?.invalidate()
  }

  /// materializedItemsDidChange coalesces one authoritative system-set refresh.
  public func materializedItemsDidChange() {
    materialization?.markDirty()
  }

  public func item(for identifier: NSFileProviderItemIdentifier) async throws
    -> CatalogFileProviderItem {
    try await bindingGate.bind()
    let object = try await client.lookup(
      tenant: binding.tenant,
      objectID: objectID(for: identifier)
    )
    return CatalogFileProviderItem(
      object: object,
      rootID: binding.rootID,
      accessMode: binding.accessMode
    )
  }

  public func fetchContents(
    for identifier: NSFileProviderItemIdentifier,
    requestedVersion: NSFileProviderItemVersion?
  ) async throws -> (URL, CatalogFileProviderItem) {
    try await bindingGate.bind()
    let objectID = try objectID(for: identifier)
    let object: CatalogObject
    do {
      object = try await client.lookup(tenant: binding.tenant, objectID: objectID)
    } catch CatalogClientError.response(.notFound, _) {
      let privateResult = try await client.lookupPrivate(
        tenant: binding.tenant,
        objectID: objectID
      )
      return try await fetchPrivateContents(
        privateResult,
        requestedVersion: requestedVersion
      )
    }
    guard object.kind == .file else { throw NSFileProviderError(.noSuchItem) }
    if let requestedVersion,
       requestedVersion.contentVersion
       != CatalogFileProviderItem(
         object: object,
         rootID: binding.rootID,
         accessMode: binding.accessMode
       ).itemVersion
       .contentVersion {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let download = try await client.open(
      tenant: binding.tenant,
      objectID: object.id,
      revision: object.revision
    )
    let url = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: false)
    guard FileManager.default.createFile(atPath: url.path, contents: nil) else {
      throw CocoaError(.fileWriteUnknown)
    }
    let (terminal, readHash) = try await materialize(download, object: object, at: url)
    do {
      if let context = try await client.resolveCriticalFetch(
        tenant: binding.tenant,
        object: terminal
      ) {
        try await client.acknowledgeCriticalFetch(
          tenant: binding.tenant,
          object: terminal,
          readHash: readHash,
          context: context
        )
      }
    } catch {
      try? FileManager.default.removeItem(at: url)
      throw error
    }
    return (
      url,
      CatalogFileProviderItem(
        object: terminal,
        rootID: binding.rootID,
        accessMode: binding.accessMode
      )
    )
  }

  private func fetchPrivateContents(
    _ result: CatalogPrivateMutationResult,
    requestedVersion: NSFileProviderItemVersion?
  ) async throws -> (URL, CatalogFileProviderItem) {
    guard result.kind == .file else { throw NSFileProviderError(.noSuchItem) }
    let item = CatalogFileProviderItem(
      privateResult: result,
      rootID: binding.rootID,
      accessMode: binding.accessMode
    )
    if let requestedVersion,
       requestedVersion.contentVersion != item.itemVersion.contentVersion {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let download = try await client.openPrivate(
      tenant: binding.tenant,
      objectID: result.objectID,
      creator: result.creator
    )
    let url = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: false)
    guard FileManager.default.createFile(atPath: url.path, contents: nil) else {
      throw CocoaError(.fileWriteUnknown)
    }
    let terminal = try await materializePrivate(download, result: result, at: url)
    return (
      url,
      CatalogFileProviderItem(
        privateResult: terminal,
        rootID: binding.rootID,
        accessMode: binding.accessMode
      )
    )
  }

  private func materialize(
    _ download: CatalogContentDownload,
    object: CatalogObject,
    at url: URL
  ) async throws -> (CatalogObject, String) {
    var file: FileHandle?
    do {
      let output = try FileHandle(forWritingTo: url)
      file = output
      var written: UInt64 = 0
      var digest = SHA256()
      while let chunk = try await download.next() {
        try output.write(contentsOf: chunk)
        digest.update(data: chunk)
        written += UInt64(chunk.count)
      }
      try output.close()
      let terminal = try await download.response()
      let actualHash = digest.finalize().map { String(format: "%02x", $0) }.joined()
      guard terminal.id == object.id,
            terminal.revision == object.revision,
            terminal.contentRevision == object.contentRevision,
            terminal.size == object.size,
            terminal.hash == object.hash,
            written == object.size,
            actualHash == object.hash
      else {
        try? FileManager.default.removeItem(at: url)
        throw CatalogClientError.response(.integrity, "stream metadata mismatch")
      }
      return (terminal, actualHash)
    } catch {
      await download.cancel()
      try? file?.close()
      try? FileManager.default.removeItem(at: url)
      throw error
    }
  }

  private func materializePrivate(
    _ download: CatalogPrivateContentDownload,
    result: CatalogPrivateMutationResult,
    at url: URL
  ) async throws -> CatalogPrivateMutationResult {
    var file: FileHandle?
    do {
      let output = try FileHandle(forWritingTo: url)
      file = output
      var written: UInt64 = 0
      var digest = SHA256()
      while let chunk = try await download.next() {
        try output.write(contentsOf: chunk)
        digest.update(data: chunk)
        written += UInt64(chunk.count)
      }
      try output.close()
      let terminal = try await download.response()
      let actualHash = digest.finalize().map { String(format: "%02x", $0) }.joined()
      guard terminal.creator == result.creator,
            terminal.objectID == result.objectID,
            terminal.parentID == result.parentID,
            terminal.name == result.name,
            terminal.kind == result.kind,
            terminal.mode == result.mode,
            terminal.createdAgainstHead == result.createdAgainstHead,
            terminal.contentRevision == result.contentRevision,
            terminal.size == result.size,
            terminal.hash == result.hash,
            terminal.linkTarget == result.linkTarget,
            written == result.size,
            actualHash == result.hash
      else {
        try? FileManager.default.removeItem(at: url)
        throw CatalogClientError.response(.integrity, "private stream metadata mismatch")
      }
      return terminal
    } catch {
      await download.cancel()
      try? file?.close()
      try? FileManager.default.removeItem(at: url)
      throw error
    }
  }
}

public extension CatalogFileProviderRuntime {
  func create(
    template: NSFileProviderItem,
    fields: NSFileProviderItemFields,
    contents: URL?
  ) async throws -> CatalogFileProviderItem {
    try await bindingGate.bind()
    try requireWritable()
    let kind: CatalogObjectKind
    let linkTarget: String?
    switch template.contentType {
    case .folder:
      kind = .directory
      linkTarget = nil
    case .symbolicLink:
      kind = .symlink
      linkTarget = template.symlinkTargetPath ?? nil
    case .data:
      kind = .file
      linkTarget = nil
    default:
      throw NSFileProviderError(.cannotSynchronize)
    }
    let hasContent = kind == .file
    guard hasContent ? contents != nil : contents == nil,
          kind != .symlink || linkTarget != nil
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let disposition = try mutationDispositionPolicy.disposition(for: template, fields: fields)
    let request = try CatalogMutationRequest(
      requestID: Self.newMutationRequestID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: .create,
      disposition: disposition,
      objectKind: kind,
      hasContent: hasContent,
      parentID: objectID(for: template.parentItemIdentifier),
      name: template.filename,
      mode: kind == .directory ? 0o755 : (kind == .symlink ? 0o777 : 0o644),
      contentRevision: kind == .directory ? nil : 1,
      linkTarget: linkTarget
    )
    let response = try await client.mutate(
      tenant: binding.tenant,
      request: request,
      content: Self.contentBody(contents)
    )
    guard let objectID = response.primaryID else {
      guard disposition == .privateStaging,
            let privateResult = response.privateResult,
            privateResult.creator == response.mutationID
      else { throw CatalogClientError.missingMutationIdentifier }
      return CatalogFileProviderItem(
        privateResult: privateResult,
        rootID: binding.rootID,
        accessMode: binding.accessMode
      )
    }
    guard disposition == .namespace, response.privateResult == nil else {
      throw CatalogClientError.mutationIdentityMismatch
    }
    return try await item(for: NSFileProviderItemIdentifier(objectID.rawValue))
  }

  func modify(
    item: NSFileProviderItem,
    baseVersion: NSFileProviderItemVersion,
    changedFields: NSFileProviderItemFields,
    contents: URL?
  ) async throws -> CatalogFileProviderItem {
    try await bindingGate.bind()
    try requireWritable()
    let sourceID = try objectID(for: item.itemIdentifier)
    let source = try await mutationSource(objectID: sourceID)
    try validate(baseVersion, matches: source)
    let expectedType: UTType =
      switch source.kind {
      case .directory: .folder
      case .file: .data
      case .symlink: .symbolicLink
      }
    guard item.contentType == expectedType,
          source.kind == .file || contents == nil,
          source.kind != .symlink || item.symlinkTargetPath ?? nil == source.linkTarget
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let destinationParent = try objectID(for: item.parentItemIdentifier)
    let target = try await mutationTarget(
      item: item,
      changedFields: changedFields,
      destinationParent: destinationParent
    )
    let replacing = target.map { $0.id != sourceID } ?? false
    let structuralChange =
      changedFields.contains(.filename)
        || changedFields.contains(.parentItemIdentifier)
    if source.privateCreator != nil, !structuralChange {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let promoting = source.privateCreator != nil && !replacing
    let hasContent = contents != nil
    let request = try CatalogMutationRequest(
      requestID: Self.newMutationRequestID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: promoting ? .promote : (replacing ? .replace : .revise),
      disposition: .namespace,
      hasContent: hasContent,
      objectID: sourceID,
      privateCreator: source.privateCreator,
      parentID: destinationParent,
      targetID: replacing ? target?.id : nil,
      name: item.filename,
      mode: source.mode,
      contentRevision: hasContent ? source.contentRevision + 1 : nil
    )
    let response = try await client.mutate(
      tenant: binding.tenant,
      request: request,
      content: Self.contentBody(contents)
    )
    guard response.primaryID == sourceID else {
      throw CatalogClientError.mutationIdentityMismatch
    }
    return try await self.item(for: NSFileProviderItemIdentifier(sourceID.rawValue))
  }

  func delete(
    identifier: NSFileProviderItemIdentifier,
    baseVersion: NSFileProviderItemVersion
  ) async throws {
    try await bindingGate.bind()
    try requireWritable()
    let objectID = try objectID(for: identifier)
    let source = try await mutationSource(objectID: objectID)
    try validate(baseVersion, matches: source)
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let request = try CatalogMutationRequest(
      requestID: Self.newMutationRequestID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: .delete,
      disposition: source.privateCreator == nil ? .namespace : .privateStaging,
      hasContent: false,
      objectID: objectID,
      privateCreator: source.privateCreator
    )
    let response = try await client.mutate(tenant: binding.tenant, request: request)
    guard response.primaryID == objectID else {
      throw CatalogClientError.mutationIdentityMismatch
    }
  }

  func enumerator(for identifier: NSFileProviderItemIdentifier) throws -> CatalogEnumerator {
    let scope: CatalogEnumerator.Scope =
      if identifier == .workingSet {
        .workingSet
      } else {
        try .container(objectID(for: identifier))
      }
    return CatalogEnumerator(
      client: client,
      binding: binding,
      scope: scope,
      activation: activation,
      bindingGate: bindingGate
    )
  }
}

extension CatalogFileProviderRuntime {
  private func requireWritable() throws {
    guard binding.accessMode == .readWrite else {
      throw NSFileProviderError(.cannotSynchronize)
    }
  }

  private func objectID(for identifier: NSFileProviderItemIdentifier) throws -> CatalogObjectID {
    if identifier == .rootContainer {
      return binding.rootID
    }
    do {
      return try CatalogObjectID(identifier.rawValue)
    } catch {
      throw NSFileProviderError(.noSuchItem)
    }
  }

  private func lookupOptional(
    tenant: CatalogTenant,
    parentID: CatalogObjectID,
    name: String
  ) async throws -> CatalogObject? {
    do {
      return try await client.lookup(tenant: tenant, parentID: parentID, name: name)
    } catch CatalogClientError.response(.notFound, _) {
      return nil
    }
  }

  private func mutationTarget(
    item: NSFileProviderItem,
    changedFields: NSFileProviderItemFields,
    destinationParent: CatalogObjectID
  ) async throws -> CatalogObject? {
    guard changedFields.contains(.filename) || changedFields.contains(.parentItemIdentifier) else {
      return nil
    }
    return try await lookupOptional(
      tenant: binding.tenant,
      parentID: destinationParent,
      name: item.filename
    )
  }

  private func mutationSource(objectID: CatalogObjectID) async throws -> MutationSource {
    do {
      return try await .namespace(
        client.lookup(tenant: binding.tenant, objectID: objectID)
      )
    } catch CatalogClientError.response(.notFound, _) {
      return try await .privateResult(
        client.lookupPrivate(tenant: binding.tenant, objectID: objectID)
      )
    }
  }

  private func validate(
    _ version: NSFileProviderItemVersion,
    matches object: CatalogObject
  ) throws {
    let current = CatalogFileProviderItem(
      object: object,
      rootID: binding.rootID,
      accessMode: binding.accessMode
    ).itemVersion
    guard version.contentVersion == current.contentVersion,
          version.metadataVersion == current.metadataVersion
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
  }

  private func validate(
    _ version: NSFileProviderItemVersion,
    matches source: MutationSource
  ) throws {
    let current = source.item(rootID: binding.rootID, accessMode: binding.accessMode).itemVersion
    guard version.contentVersion == current.contentVersion,
          version.metadataVersion == current.metadataVersion
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
  }

  private static func newMutationRequestID() throws -> CatalogMutationRequestID {
    try CatalogMutationRequestID(
      UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    )
  }

  private static func contentBody(_ url: URL?) throws -> CatalogUpload {
    guard let url else { return .empty }
    let cursor = try CatalogFileUploadCursor(url: url)
    return CatalogUpload(
      next: { try await cursor.next() },
      cancel: { await cursor.cancel() }
    )
  }
}

private enum MutationSource {
  case namespace(CatalogObject)
  case privateResult(CatalogPrivateMutationResult)

  var privateCreator: CatalogMutationID? {
    switch self {
    case .namespace: nil
    case let .privateResult(result): result.creator
    }
  }

  var kind: CatalogObjectKind {
    switch self {
    case let .namespace(object): object.kind
    case let .privateResult(result): result.kind
    }
  }

  var mode: UInt32 {
    switch self {
    case let .namespace(object): object.mode
    case let .privateResult(result): result.mode
    }
  }

  var contentRevision: UInt64 {
    switch self {
    case let .namespace(object): object.contentRevision
    case let .privateResult(result): result.contentRevision
    }
  }

  var linkTarget: String {
    switch self {
    case let .namespace(object): object.linkTarget
    case let .privateResult(result): result.linkTarget
    }
  }

  func item(
    rootID: CatalogObjectID,
    accessMode: CatalogTenantAccessMode
  ) -> CatalogFileProviderItem {
    switch self {
    case let .namespace(object):
      CatalogFileProviderItem(object: object, rootID: rootID, accessMode: accessMode)
    case let .privateResult(result):
      CatalogFileProviderItem(privateResult: result, rootID: rootID, accessMode: accessMode)
    }
  }
}
