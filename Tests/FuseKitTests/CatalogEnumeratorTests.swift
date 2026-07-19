@preconcurrency import FileProvider
import Foundation
import Testing

@testable import FuseKit

@Suite("Catalog change enumeration")
struct CatalogEnumeratorTests {
  @Test
  func observerFinishesBeforeExactAcknowledgementAndTaskDrains() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(recorder: recorder, failAcknowledgement: false)
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: CatalogEnumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()

    #expect(recorder.values() == ["finish", "ack"])
    #expect(observer.errors() == 0)
    #expect(await fixture.transport.acknowledgements() == [7])
  }

  @Test
  func acknowledgementFailureAfterFinishDoesNotSendSecondObserverTerminal() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(recorder: recorder, failAcknowledgement: true)
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: CatalogEnumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()

    #expect(recorder.values() == ["finish", "ack"])
    #expect(observer.finishes() == 1)
    #expect(observer.errors() == 0)
    await #expect(
      throws: CatalogConvergenceInbox.InboxError.notificationStreamFailed("acknowledgement")
    ) {
      try await fixture.inbox.checkHealthy()
    }
  }

  @Test
  func oneRevisionPaginatesBySequenceWithoutEarlyAcknowledgement() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(
      recorder: recorder,
      failAcknowledgement: false,
      paginated: true
    )
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: CatalogEnumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()
    #expect(await fixture.transport.acknowledgements().isEmpty)
    #expect(observer.moreComingValues() == [true])

    let next = try #require(observer.lastAnchor())
    fixture.enumerator.enumerateChanges(for: observer, from: next)
    await fixture.waitUntilDrained()

    #expect(observer.updateCount() == 2)
    #expect(observer.moreComingValues() == [true, false])
    #expect(await fixture.transport.requestedCursors() == ["6:4294967295", "7:1"])
    #expect(await fixture.transport.acknowledgements() == [7])
    #expect(recorder.values() == ["finish", "finish", "ack"])
  }
}

private enum CatalogEnumeratorTestError: Error, Equatable {
  case acknowledgement
}

private struct EnumeratorFixture {
  let transport: EnumeratorTransport
  let inbox: CatalogConvergenceInbox
  let notification: CatalogConvergenceNotification
  let enumerator: CatalogEnumerator

  init(
    recorder: OrderingRecorder,
    failAcknowledgement: Bool,
    paginated: Bool = false
  ) throws {
    let binding = try CatalogFileProviderBinding(
      domainID: CatalogDomainID.derived(
        ownerID: CatalogOwnerID("owner-1"),
        accountInstanceID: CatalogAccountInstanceID("account-1")
      ),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 3),
      rootID: CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    )
    let transport = try EnumeratorTransport(
      recorder: recorder,
      failAcknowledgement: failAcknowledgement,
      binding: binding,
      paginatedObject: paginated
        ? CatalogObject(
          id: CatalogObjectID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
          parentID: binding.rootID,
          revision: 7,
          metadataRevision: 7,
          contentRevision: 1,
          name: "settings.json",
          kind: .file,
          mode: 0o644,
          size: 0,
          hash: "",
          linkTarget: "",
          desired: 7,
          observed: 7,
          verified: 7,
          applied: 7,
          tombstone: false
        ) : nil
    )
    let client = CatalogClient(transport: transport)
    let inbox = CatalogConvergenceInbox(binding: binding, client: client)
    self.transport = transport
    self.inbox = inbox
    notification = try CatalogConvergenceNotification(
      tenantID: binding.tenant.identifier,
      domainID: binding.domainID,
      generation: binding.tenant.generation,
      revision: 7,
      catalogRevision: 7,
      sourceAuthority: CatalogSourceAuthorityID("source-main"),
      sourceRevision: 5,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogMutationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      affectedKeys: ["settings.json"],
      targets: [CatalogSignalTarget(kind: .workingSet)]
    )
    enumerator = CatalogEnumerator(
      client: client,
      binding: binding,
      scope: .workingSet,
      convergence: inbox,
      bindingGate: CatalogBindingGate(binding: binding, client: client)
    )
  }

  func waitUntilDrained() async {
    while enumerator.activeTaskCount() != 0 {
      await Task.yield()
    }
  }
}

private final class OrderingRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []

  func append(_ value: String) {
    lock.withLock { recorded.append(value) }
  }

  func values() -> [String] {
    lock.withLock { recorded }
  }
}

private final class RecordingChangeObserver: NSObject, NSFileProviderChangeObserver,
  @unchecked Sendable
{
  private let recorder: OrderingRecorder
  private let lock = NSLock()
  private var finishCount = 0
  private var errorCount = 0
  private var anchors: [NSFileProviderSyncAnchor] = []
  private var moreComing: [Bool] = []
  private var updates = 0

  init(recorder: OrderingRecorder) {
    self.recorder = recorder
  }

  func didUpdate(_ items: [any NSFileProviderItem]) {
    lock.withLock { updates += items.count }
  }

  func didDeleteItems(withIdentifiers _: [NSFileProviderItemIdentifier]) {}

  func finishEnumeratingChanges(
    upTo anchor: NSFileProviderSyncAnchor,
    moreComing value: Bool
  ) {
    lock.withLock {
      finishCount += 1
      anchors.append(anchor)
      moreComing.append(value)
    }
    recorder.append("finish")
  }

  func finishEnumeratingWithError(_: any Error) {
    lock.withLock { errorCount += 1 }
    recorder.append("error")
  }

  func finishes() -> Int {
    lock.withLock { finishCount }
  }

  func errors() -> Int {
    lock.withLock { errorCount }
  }

  func lastAnchor() -> NSFileProviderSyncAnchor? {
    lock.withLock { anchors.last }
  }

  func moreComingValues() -> [Bool] {
    lock.withLock { moreComing }
  }

  func updateCount() -> Int {
    lock.withLock { updates }
  }
}

private actor EnumeratorTransport: CatalogTransport {
  private let recorder: OrderingRecorder
  private let failAcknowledgement: Bool
  private let binding: CatalogFileProviderBinding
  private let paginatedObject: CatalogObject?
  private var acked: [UInt64] = []
  private var cursors: [String] = []

  init(
    recorder: OrderingRecorder,
    failAcknowledgement: Bool,
    binding: CatalogFileProviderBinding,
    paginatedObject: CatalogObject?
  ) {
    self.recorder = recorder
    self.failAcknowledgement = failAcknowledgement
    self.binding = binding
    self.paginatedObject = paginatedObject
  }

  func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    guard domainID == binding.domainID, tenant == binding.tenant else {
      throw CatalogTransportError.bindingRequired
    }
  }

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .catalogChangesSince:
      let request = try decoder.decode(CatalogChangesSinceRequest.self, from: payload)
      cursors.append("\(request.cursor.revision):\(request.cursor.sequence)")
      if let object = paginatedObject, request.cursor.revision == 6 {
        return try encoder.encode(
          CatalogChangesSinceResponse(
            code: .ok,
            message: "",
            floor: 1,
            head: 7,
            next: CatalogChangeCursor(revision: 7, sequence: 1),
            complete: false,
            changes: [CatalogChange(revision: 7, sequence: 1, kind: .upsert, object: object)]
          )
        )
      }
      if let object = paginatedObject {
        return try encoder.encode(
          CatalogChangesSinceResponse(
            code: .ok,
            message: "",
            floor: 1,
            head: 7,
            next: CatalogChangeCursor(
              revision: 7,
              sequence: CatalogProtocol.changeCursorCompleteSequence
            ),
            complete: true,
            changes: [CatalogChange(revision: 7, sequence: 2, kind: .upsert, object: object)]
          )
        )
      }
      return try encoder.encode(
        CatalogChangesSinceResponse(
          code: .ok,
          message: "",
          floor: 1,
          head: 7,
          next: CatalogChangeCursor(
            revision: 7,
            sequence: CatalogProtocol.changeCursorCompleteSequence
          ),
          complete: true,
          changes: []
        )
      )
    case .convergenceAck:
      let request = try decoder.decode(CatalogAckConvergenceRequest.self, from: payload)
      recorder.append("ack")
      if failAcknowledgement {
        throw CatalogEnumeratorTestError.acknowledgement
      }
      acked.append(request.revision)
      return try encoder.encode(
        CatalogAckConvergenceResponse(
          code: .ok,
          message: "",
          observation: CatalogDomainObservation(
            tenantID: CatalogTenantID(tenant),
            domainID: request.domainID,
            generation: request.generation,
            requestedRevision: request.revision,
            observedRevision: request.revision,
            catalogRevision: request.catalogRevision,
            sourceAuthority: request.sourceAuthority,
            sourceRevision: request.sourceRevision,
            changeID: request.changeID,
            operationID: request.operationID
          )
        )
      )
    default:
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
  }

  func download(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    throw CatalogTransportError.remote("unexpected download")
  }

  func upload(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }

  func acknowledgements() -> [UInt64] {
    acked
  }

  func requestedCursors() -> [String] {
    cursors
  }
}
