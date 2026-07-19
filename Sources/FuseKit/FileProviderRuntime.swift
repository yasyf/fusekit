import CryptoKit
import FileProvider
import Foundation
import UniformTypeIdentifiers

/// CatalogFileProviderRuntime owns generic lookup, fetch, enumeration, and mutation behavior.
public final class CatalogFileProviderRuntime: Sendable {
  public let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private let convergence: CatalogConvergenceInbox
  private let bindingGate: CatalogBindingGate
  private let notificationTask: Task<Void, Never>

  public init(binding: CatalogFileProviderBinding, client: CatalogClient) {
    self.binding = binding
    self.client = client
    let bindingGate = CatalogBindingGate(binding: binding, client: client)
    self.bindingGate = bindingGate
    let convergence = CatalogConvergenceInbox(binding: binding, client: client)
    self.convergence = convergence
    notificationTask = Task {
      let notifications = client.convergenceNotifications()
      do {
        try await bindingGate.bind()
        while let notification = try await notifications.next() {
          try await convergence.receive(notification)
        }
      } catch is CancellationError {
        await notifications.cancel()
      } catch {
        await notifications.cancel()
        await convergence.fail(error)
      }
    }
  }

  deinit {
    notificationTask.cancel()
  }

  /// invalidate stops the persistent convergence event consumer.
  public func invalidate() {
    notificationTask.cancel()
  }

  public func item(for identifier: NSFileProviderItemIdentifier) async throws
    -> CatalogFileProviderItem {
    try await bindingGate.bind()
    let object = try await client.lookup(
      tenant: binding.tenant,
      objectID: objectID(for: identifier)
    )
    return CatalogFileProviderItem(object: object, rootID: binding.rootID)
  }

  public func fetchContents(
    for identifier: NSFileProviderItemIdentifier,
    requestedVersion: NSFileProviderItemVersion?
  ) async throws -> (URL, CatalogFileProviderItem) {
    try await bindingGate.bind()
    let objectID = try objectID(for: identifier)
    let object = try await client.lookup(tenant: binding.tenant, objectID: objectID)
    guard object.kind == .file else { throw NSFileProviderError(.noSuchItem) }
    if let requestedVersion,
       requestedVersion.contentVersion
       != CatalogFileProviderItem(object: object, rootID: binding.rootID).itemVersion
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
    let file = try FileHandle(forWritingTo: url)
    do {
      var written: UInt64 = 0
      var digest = SHA256()
      while let chunk = try await download.next() {
        try file.write(contentsOf: chunk)
        digest.update(data: chunk)
        written += UInt64(chunk.count)
      }
      try file.close()
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
      return (url, CatalogFileProviderItem(object: terminal, rootID: binding.rootID))
    } catch {
      await download.cancel()
      try? file.close()
      try? FileManager.default.removeItem(at: url)
      throw error
    }
  }
}

public extension CatalogFileProviderRuntime {
  func create(
    template: NSFileProviderItem,
    contents: URL?
  ) async throws -> CatalogFileProviderItem {
    try await bindingGate.bind()
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
    let request = try CatalogMutationRequest(
      operationID: Self.newMutationID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: .create,
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
      throw CatalogClientError.missingMutationIdentifier
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
    let sourceID = try objectID(for: item.itemIdentifier)
    let source = try await client.lookup(tenant: binding.tenant, objectID: sourceID)
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
    let hasContent = contents != nil
    let request = try CatalogMutationRequest(
      operationID: Self.newMutationID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: replacing ? .replace : .revise,
      hasContent: hasContent,
      objectID: sourceID,
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
    let objectID = try objectID(for: identifier)
    let object = try await client.lookup(tenant: binding.tenant, objectID: objectID)
    try validate(baseVersion, matches: object)
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let request = try CatalogMutationRequest(
      operationID: Self.newMutationID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: .delete,
      hasContent: false,
      objectID: objectID
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
      convergence: convergence,
      bindingGate: bindingGate
    )
  }
}

extension CatalogFileProviderRuntime {
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

  private func validate(
    _ version: NSFileProviderItemVersion,
    matches object: CatalogObject
  ) throws {
    let current = CatalogFileProviderItem(object: object, rootID: binding.rootID).itemVersion
    guard version.contentVersion == current.contentVersion,
          version.metadataVersion == current.metadataVersion
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
  }

  private static func newMutationID() throws -> CatalogMutationID {
    try CatalogMutationID(UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased())
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
