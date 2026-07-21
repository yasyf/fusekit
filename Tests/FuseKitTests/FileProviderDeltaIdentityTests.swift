@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

@Suite("File Provider replacement deltas")
struct FileProviderDeltaIdentityTests {
  @Test
  func replaceDeletesTargetOnceAndUpsertsTemporaryIdentityWithoutSnapshotWork() async throws {
    let fixture = try ReplacementDeltaFixture()
    let transport = fixture.transport
    let client = CatalogClient(transport: transport)
    let enumerator = CatalogEnumerator(
      client: client,
      binding: fixture.binding,
      scope: .workingSet,
      convergence: CatalogConvergenceInbox(binding: fixture.binding, client: client),
      bindingGate: CatalogBindingGate(binding: fixture.binding, client: client)
    )
    let observer = ReplacementChangeObserver()

    enumerator.enumerateChanges(
      for: observer,
      from: enumerator.anchor(
        CatalogChangeCursor(
          revision: 7,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    while enumerator.activeTaskCount() != 0 {
      await Task.yield()
    }

    #expect(observer.deleted() == [fixture.targetID.rawValue])
    #expect(observer.updated() == [fixture.temporaryID.rawValue])
    #expect(observer.finishes() == 1)
    #expect(observer.errors() == 0)
    #expect(await transport.calls() == [CatalogOperation.catalogChangesSince.rawValue])
  }
}

private struct ReplacementDeltaFixture {
  let binding: CatalogFileProviderBinding
  let targetID: CatalogObjectID
  let temporaryID: CatalogObjectID
  let transport: ReplacementDeltaTransport

  init() throws {
    let rootID = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    targetID = try CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    temporaryID = try CatalogObjectID("cccccccccccccccccccccccccccccccc")
    binding = try CatalogFileProviderBinding(
      domainID: CatalogDomainID.derived(
        ownerID: CatalogOwnerID("owner-delta"),
        accountInstanceID: CatalogAccountInstanceID("account-delta")
      ),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-delta"), generation: 4),
      rootID: rootID,
      accessMode: .readWrite
    )
    let target = try deltaObject(
      id: targetID,
      parent: rootID,
      name: "settings.json",
      revision: 8,
      tombstone: true
    )
    let replacement = try deltaObject(
      id: temporaryID,
      parent: rootID,
      name: "settings.json",
      revision: 8,
      tombstone: false
    )
    transport = ReplacementDeltaTransport(
      binding: binding,
      changes: [
        CatalogChange(revision: 8, sequence: 1, kind: .delete, object: target),
        CatalogChange(revision: 8, sequence: 2, kind: .upsert, object: replacement),
      ]
    )
  }
}

private func deltaObject(
  id: CatalogObjectID,
  parent: CatalogObjectID,
  name: String,
  revision: UInt64,
  tombstone: Bool
) throws -> CatalogObject {
  try CatalogObject(
    id: id,
    parentID: parent,
    revision: revision,
    metadataRevision: revision,
    contentRevision: 1,
    name: name,
    kind: .file,
    mode: 0o600,
    size: 0,
    hash: "",
    linkTarget: "",
    desired: revision,
    observed: revision,
    verified: revision,
    applied: revision,
    tombstone: tombstone
  )
}

private actor ReplacementDeltaTransport: CatalogTransport {
  private let binding: CatalogFileProviderBinding
  private let changes: [CatalogChange]
  private var operations: [String] = []

  init(binding: CatalogFileProviderBinding, changes: [CatalogChange]) {
    self.binding = binding
    self.changes = changes
  }

  func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    guard domainID == binding.domainID, tenant == binding.tenant else {
      throw CatalogTransportError.bindingRequired
    }
  }

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload: Data) async throws -> Data {
    operations.append(operation.rawValue)
    guard operation == .catalogChangesSince else {
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
    let request = try JSONDecoder().decode(CatalogChangesSinceRequest.self, from: payload)
    guard request.generation == binding.tenant.generation,
          request.scope.kind == .workingSet,
          request.cursor.revision == 7,
          request.cursor.sequence == CatalogProtocol.changeCursorCompleteSequence
    else {
      throw CatalogTransportError.remote("invalid change request")
    }
    return try JSONEncoder().encode(
      CatalogChangesSinceResponse(
        code: .ok,
        message: "",
        floor: 1,
        head: 8,
        next: CatalogChangeCursor(
          revision: 8,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        ),
        complete: true,
        changes: changes
      )
    )
  }

  func download(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    operations.append(operation.rawValue)
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    operations.append(operation.rawValue)
    throw CatalogTransportError.remote("unexpected upload")
  }

  func calls() -> [String] {
    operations
  }
}

private final class ReplacementChangeObserver: NSObject, NSFileProviderChangeObserver,
  @unchecked Sendable {
  private let lock = NSLock()
  private var deletedIDs: [String] = []
  private var updatedIDs: [String] = []
  private var finishCount = 0
  private var errorCount = 0

  func didUpdate(_ items: [any NSFileProviderItem]) {
    lock.withLock {
      updatedIDs.append(contentsOf: items.map(\.itemIdentifier.rawValue))
    }
  }

  func didDeleteItems(withIdentifiers identifiers: [NSFileProviderItemIdentifier]) {
    lock.withLock {
      deletedIDs.append(contentsOf: identifiers.map(\.rawValue))
    }
  }

  func finishEnumeratingChanges(
    upTo _: NSFileProviderSyncAnchor,
    moreComing _: Bool
  ) {
    lock.withLock { finishCount += 1 }
  }

  func finishEnumeratingWithError(_: any Error) {
    lock.withLock { errorCount += 1 }
  }

  func deleted() -> [String] {
    lock.withLock { deletedIDs }
  }

  func updated() -> [String] {
    lock.withLock { updatedIDs }
  }

  func finishes() -> Int {
    lock.withLock { finishCount }
  }

  func errors() -> Int {
    lock.withLock { errorCount }
  }
}
