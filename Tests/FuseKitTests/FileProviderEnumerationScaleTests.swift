@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

@Suite("File Provider catalog paging")
struct FileProviderEnumerationScaleTests {
  @Test
  func tenThousandItemsUseOneHeadAndOnlyImmutableSnapshotPages() async throws {
    let fixture = try EnumerationScaleFixture(count: 10000, pageSize: 256)
    var page = NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
    var identifiers: [String] = []

    while true {
      let observer = RecordingEnumerationObserver()
      fixture.enumerator.enumerateItems(for: observer, startingAt: page)
      await fixture.waitUntilDrained()
      #expect(observer.errors() == 0)
      identifiers.append(contentsOf: observer.identifiers())
      guard let next = observer.nextPage() else { break }
      page = next
    }

    #expect(identifiers.count == 10000)
    #expect(Set(identifiers).count == 10000)
    #expect(identifiers == fixture.expectedIdentifiers)
    let calls = await fixture.transport.calls()
    #expect(calls.filter { $0 == CatalogOperation.catalogHead.rawValue }.count == 1)
    #expect(calls.filter { $0 == CatalogOperation.catalogSnapshot.rawValue }.count == 40)
    #expect(
      calls.allSatisfy {
        $0 == CatalogOperation.catalogHead.rawValue
          || $0 == CatalogOperation.catalogSnapshot.rawValue
      }
    )
  }

  @Test
  func currentAnchorIsOneHeadCallAndNoSnapshotOrContentRead() async throws {
    let fixture = try EnumerationScaleFixture(count: 10000, pageSize: 256)
    let anchor: NSFileProviderSyncAnchor? = await withCheckedContinuation { continuation in
      fixture.enumerator.currentSyncAnchor { continuation.resume(returning: $0) }
    }
    await fixture.waitUntilDrained()

    let value = try #require(anchor)
    let cursor = try fixture.enumerator.decodeAnchor(value)
    #expect(cursor.revision == EnumerationScaleTransport.revision)
    #expect(cursor.sequence == CatalogProtocol.changeCursorCompleteSequence)
    #expect(await fixture.transport.calls() == [CatalogOperation.catalogHead.rawValue])
  }

  @Test
  func pageReplayAcrossTenantGenerationExpiresBeforeSnapshotRead() async throws {
    let source = try EnumerationScaleFixture(count: 2, pageSize: 1, generation: 9)
    let target = try EnumerationScaleFixture(count: 2, pageSize: 1, generation: 10)
    let first = RecordingEnumerationObserver()
    source.enumerator.enumerateItems(
      for: first,
      startingAt: NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
    )
    await source.waitUntilDrained()
    let token = try #require(first.nextPage())
    let replay = RecordingEnumerationObserver()

    target.enumerator.enumerateItems(for: replay, startingAt: token)
    await target.waitUntilDrained()

    #expect(replay.errorCodes() == [NSFileProviderError.Code.pageExpired.rawValue])
    #expect(await target.transport.calls().isEmpty)
  }

  @Test
  func pageReplayAcrossEnumerationScopeExpiresBeforeSnapshotRead() async throws {
    let source = try EnumerationScaleFixture(count: 2, pageSize: 1)
    let target = try EnumerationScaleFixture(count: 2, pageSize: 1, workingSet: true)
    let first = RecordingEnumerationObserver()
    source.enumerator.enumerateItems(
      for: first,
      startingAt: NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
    )
    await source.waitUntilDrained()
    let token = try #require(first.nextPage())
    let replay = RecordingEnumerationObserver()

    target.enumerator.enumerateItems(for: replay, startingAt: token)
    await target.waitUntilDrained()

    #expect(replay.errorCodes() == [NSFileProviderError.Code.pageExpired.rawValue])
    #expect(await target.transport.calls().isEmpty)
  }

  @Test
  func pageReplayAcrossDomainTenantOrRootExpiresBeforeSnapshotRead() async throws {
    let source = try EnumerationScaleFixture(count: 2, pageSize: 1)
    let first = RecordingEnumerationObserver()
    source.enumerator.enumerateItems(
      for: first,
      startingAt: NSFileProviderPage(NSFileProviderPage.initialPageSortedByName as Data)
    )
    await source.waitUntilDrained()
    let token = try #require(first.nextPage())
    let targets = try [
      EnumerationScaleFixture(
        count: 2,
        pageSize: 1,
        owner: "owner-other",
        account: "account-other"
      ),
      EnumerationScaleFixture(count: 2, pageSize: 1, tenantID: "tenant-other"),
      EnumerationScaleFixture(
        count: 2,
        pageSize: 1,
        rootID: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      ),
    ]

    for target in targets {
      let replay = RecordingEnumerationObserver()
      target.enumerator.enumerateItems(for: replay, startingAt: token)
      await target.waitUntilDrained()
      #expect(replay.errorCodes() == [NSFileProviderError.Code.pageExpired.rawValue])
      #expect(await target.transport.calls().isEmpty)
    }
  }

  @Test
  func bindingRejectsPageSizesOutsideCatalogProtocolLimit() throws {
    let rootID = try CatalogObjectID("ffffffffffffffffffffffffffffffff")
    let tenant = try CatalogTenant(identifier: CatalogTenantID("tenant-scale"), generation: 9)
    let domainID = try CatalogDomainID.derived(
      ownerID: CatalogOwnerID("owner-scale"),
      accountInstanceID: CatalogAccountInstanceID("account-scale")
    )

    #expect(throws: CatalogFileProviderBindingError.invalidPageSize) {
      _ = try CatalogFileProviderBinding(
        domainID: domainID,
        tenant: tenant,
        rootID: rootID,
        accessMode: .readWrite,
        pageSize: 0
      )
    }
    #expect(throws: CatalogFileProviderBindingError.invalidPageSize) {
      _ = try CatalogFileProviderBinding(
        domainID: domainID,
        tenant: tenant,
        rootID: rootID,
        accessMode: .readWrite,
        pageSize: CatalogFileProviderBinding.maximumPageSize + 1
      )
    }
  }
}

private struct EnumerationScaleFixture {
  let transport: EnumerationScaleTransport
  let enumerator: CatalogEnumerator
  let expectedIdentifiers: [String]

  init(
    count: Int,
    pageSize: UInt32,
    generation: UInt64 = 9,
    workingSet: Bool = false,
    owner: String = "owner-scale",
    account: String = "account-scale",
    tenantID: String = "tenant-scale",
    rootID rootValue: String = "ffffffffffffffffffffffffffffffff"
  ) throws {
    let rootID = try CatalogObjectID(rootValue)
    let binding = try CatalogFileProviderBinding(
      domainID: CatalogDomainID.derived(
        ownerID: CatalogOwnerID(owner),
        accountInstanceID: CatalogAccountInstanceID(account)
      ),
      tenant: CatalogTenant(
        identifier: CatalogTenantID(tenantID),
        generation: generation
      ),
      rootID: rootID,
      accessMode: .readWrite,
      pageSize: pageSize
    )
    var objects: [CatalogObject] = []
    objects.reserveCapacity(count)
    for index in 0 ..< count {
      let identifier = String(format: "%032x", index + 1)
      try objects.append(
        CatalogObject(
          id: CatalogObjectID(identifier),
          parentID: rootID,
          revision: EnumerationScaleTransport.revision,
          metadataRevision: 1,
          contentRevision: 1,
          name: String(format: "item-%05d", index),
          kind: .file,
          mode: 0o644,
          size: 0,
          hash: "",
          linkTarget: "",
          desired: EnumerationScaleTransport.revision,
          observed: EnumerationScaleTransport.revision,
          verified: EnumerationScaleTransport.revision,
          applied: EnumerationScaleTransport.revision,
          tombstone: false
        )
      )
    }
    let transport = EnumerationScaleTransport(binding: binding, objects: objects)
    let client = CatalogClient(transport: transport)
    self.transport = transport
    expectedIdentifiers = objects.map(\.id.rawValue)
    enumerator = CatalogEnumerator(
      client: client,
      binding: binding,
      scope: workingSet ? .workingSet : .container(rootID),
      convergence: CatalogConvergenceInbox(binding: binding, client: client),
      bindingGate: CatalogBindingGate(binding: binding, client: client)
    )
  }

  func waitUntilDrained() async {
    while enumerator.activeTaskCount() != 0 {
      await Task.yield()
    }
  }
}

private actor EnumerationScaleTransport: CatalogTransport {
  static let revision: UInt64 = 42

  private let binding: CatalogFileProviderBinding
  private let objects: [CatalogObject]
  private var recordedCalls: [String] = []

  init(binding: CatalogFileProviderBinding, objects: [CatalogObject]) {
    self.binding = binding
    self.objects = objects
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
    recordedCalls.append(operation.rawValue)
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .catalogHead:
      let request = try decoder.decode(CatalogHeadRequest.self, from: payload)
      guard request.generation == binding.tenant.generation else {
        throw CatalogTransportError.remote("wrong generation")
      }
      return try encoder.encode(
        CatalogHeadResponse(code: .ok, message: "", revision: Self.revision)
      )
    case .catalogSnapshot:
      let request = try decoder.decode(CatalogSnapshotRequest.self, from: payload)
      guard request.generation == binding.tenant.generation,
            request.revision == Self.revision,
            request.scope.kind == .container,
            request.scope.parentID == binding.rootID,
            request.limit == binding.pageSize
      else {
        throw CatalogTransportError.remote("invalid snapshot request")
      }
      let start: Int
      if let after = request.after {
        guard let index = objects.firstIndex(where: { $0.id == after }) else {
          throw CatalogTransportError.remote("unknown snapshot cursor")
        }
        start = index + 1
      } else {
        start = 0
      }
      let end = min(start + Int(request.limit), objects.count)
      let page = Array(objects[start ..< end])
      let next = end < objects.count ? page.last?.id : nil
      return try encoder.encode(
        CatalogSnapshotResponse(
          code: .ok,
          message: "",
          revision: Self.revision,
          objects: page,
          next: next
        )
      )
    default:
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
  }

  func download(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    recordedCalls.append(operation.rawValue)
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    recordedCalls.append(operation.rawValue)
    throw CatalogTransportError.remote("unexpected upload")
  }

  func calls() -> [String] {
    recordedCalls
  }
}

private final class RecordingEnumerationObserver: NSObject, NSFileProviderEnumerationObserver,
  @unchecked Sendable {
  private let lock = NSLock()
  private var values: [String] = []
  private var next: NSFileProviderPage?
  private var errorCount = 0
  private var recordedErrorCodes: [Int] = []

  func didEnumerate(_ updatedItems: [any NSFileProviderItem]) {
    lock.withLock {
      values.append(contentsOf: updatedItems.map(\.itemIdentifier.rawValue))
    }
  }

  func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
    lock.withLock { next = nextPage }
  }

  func finishEnumeratingWithError(_ error: any Error) {
    lock.withLock {
      errorCount += 1
      recordedErrorCodes.append((error as NSError).code)
    }
  }

  func identifiers() -> [String] {
    lock.withLock { values }
  }

  func nextPage() -> NSFileProviderPage? {
    lock.withLock { next }
  }

  func errors() -> Int {
    lock.withLock { errorCount }
  }

  func errorCodes() -> [Int] {
    lock.withLock { recordedErrorCodes }
  }
}
