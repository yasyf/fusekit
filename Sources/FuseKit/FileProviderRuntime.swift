import CryptoKit
import FileProvider
import Foundation
import UniformTypeIdentifiers

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
}

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
    -> CatalogFileProviderItem
  {
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
       .contentVersion
    {
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

  public func create(
    template: NSFileProviderItem,
    contents: URL?
  ) async throws -> CatalogFileProviderItem {
    try await bindingGate.bind()
    let isDirectory = template.contentType == .folder
    guard isDirectory ? contents == nil : contents != nil else {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let hasContent = !isDirectory
    let request = try CatalogMutationRequest(
      operationID: Self.newMutationID(),
      generation: binding.tenant.generation,
      expectedRevision: expectedRevision,
      kind: .create,
      objectKind: isDirectory ? .directory : .file,
      hasContent: hasContent,
      parentID: objectID(for: template.parentItemIdentifier),
      name: template.filename,
      mode: isDirectory ? 0o755 : 0o644,
      contentRevision: hasContent ? 1 : nil
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

  public func modify(
    item: NSFileProviderItem,
    baseVersion: NSFileProviderItemVersion,
    changedFields: NSFileProviderItemFields,
    contents: URL?
  ) async throws -> CatalogFileProviderItem {
    try await bindingGate.bind()
    let sourceID = try objectID(for: item.itemIdentifier)
    let source = try await client.lookup(tenant: binding.tenant, objectID: sourceID)
    try validate(baseVersion, matches: source)
    let expectedType: UTType = source.kind == .directory ? .folder : .data
    guard item.contentType == expectedType,
          source.kind == .file || contents == nil
    else {
      throw NSFileProviderError(.cannotSynchronize)
    }
    let expectedRevision = try await client.head(tenant: binding.tenant)
    let destinationParent = try objectID(for: item.parentItemIdentifier)
    let target: CatalogObject? =
      if changedFields.contains(.filename) || changedFields.contains(.parentItemIdentifier) {
        try await lookupOptional(
          tenant: binding.tenant,
          parentID: destinationParent,
          name: item.filename
        )
      } else {
        nil
      }
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

  public func delete(
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

  public func enumerator(for identifier: NSFileProviderItemIdentifier) throws -> CatalogEnumerator {
    let scope: CatalogEnumerator.Scope = if identifier == .workingSet {
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

actor CatalogBindingGate {
  private let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private var task: Task<Void, Error>?
  private var bound = false

  init(binding: CatalogFileProviderBinding, client: CatalogClient) {
    self.binding = binding
    self.client = client
  }

  func bind() async throws {
    if bound {
      return
    }
    if let task {
      return try await task.value
    }
    let task = Task {
      try await client.bind(domainID: binding.domainID, tenant: binding.tenant)
    }
    self.task = task
    do {
      try await task.value
      bound = true
      self.task = nil
    } catch {
      self.task = nil
      throw error
    }
  }
}

private actor CatalogFileUploadCursor {
  private let handle: FileHandle
  private var finished = false

  init(url: URL) throws {
    handle = try FileHandle(forReadingFrom: url)
  }

  func next() throws -> Data? {
    guard !finished else { return nil }
    do {
      guard let data = try handle.read(upToCount: 1024 * 1024), !data.isEmpty else {
        finished = true
        try handle.close()
        return nil
      }
      return data
    } catch {
      finished = true
      try? handle.close()
      throw error
    }
  }

  func cancel() {
    guard !finished else { return }
    finished = true
    try? handle.close()
  }
}
