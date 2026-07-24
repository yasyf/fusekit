@preconcurrency import FileProvider
import Foundation

@testable import FuseKit

struct EnumeratorFixture {
  let transport: EnumeratorTransport
  let inbox: CatalogActivationInbox
  let notification: CatalogActivationNotification
  let enumerator: CatalogEnumerator

  init(
    recorder: OrderingRecorder,
    failAcknowledgement: Bool,
    paginated: Bool = false,
    generation: UInt64 = 3,
    scope: CatalogEnumerator.Scope = .workingSet,
    owner: String = "owner-1",
    account: String = "account-1",
    tenantID: String = "tenant-1",
    rootID: String = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  ) throws {
    let binding = try Self.binding(
      generation: generation,
      owner: owner,
      account: account,
      tenantID: tenantID,
      rootID: rootID
    )
    let transport = try EnumeratorTransport(
      recorder: recorder,
      failAcknowledgement: failAcknowledgement,
      binding: binding,
      paginatedObject: Self.paginatedObject(binding: binding, enabled: paginated)
    )
    let client = CatalogClient(transport: transport)
    let inbox = CatalogActivationInbox(binding: binding, client: client)
    self.transport = transport
    self.inbox = inbox
    notification = try Self.notification(binding: binding)
    enumerator = CatalogEnumerator(
      client: client,
      binding: binding,
      scope: scope,
      activation: inbox,
      bindingGate: CatalogBindingGate(binding: binding, client: client)
    )
  }

  private static func binding(
    generation: UInt64,
    owner: String,
    account: String,
    tenantID: String,
    rootID: String
  ) throws -> CatalogFileProviderBinding {
    try CatalogFileProviderBinding(
      domainID: CatalogDomainID.derived(
        ownerID: CatalogOwnerID(owner),
        presentationInstanceID: CatalogPresentationInstanceID(account)
      ),
      tenant: CatalogTenant(identifier: CatalogTenantID(tenantID), generation: generation),
      rootID: CatalogObjectID(rootID),
      accessMode: .readWrite
    )
  }

  private static func paginatedObject(
    binding: CatalogFileProviderBinding,
    enabled: Bool
  ) throws -> CatalogObject? {
    guard enabled else { return nil }
    return try CatalogObject(
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
    )
  }

  private static func notification(
    binding: CatalogFileProviderBinding
  ) throws -> CatalogActivationNotification {
    try testActivationNotification(
      tenantID: binding.tenant.identifier, domainID: binding.domainID,
      generation: binding.tenant.generation, activationRevision: 7, catalogHead: 7,
      sourceRevision: 5,
      targetCount: 1,
      targets: [CatalogSignalTarget(kind: .workingSet)]
    )
  }

  func waitUntilDrained() async {
    while enumerator.activeTaskCount() != 0 {
      await Task.yield()
    }
  }
}

final class OrderingRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []

  func append(_ value: String) {
    lock.withLock { recorded.append(value) }
  }

  func values() -> [String] {
    lock.withLock { recorded }
  }
}

final class RecordingChangeObserver: NSObject, NSFileProviderChangeObserver,
  @unchecked Sendable
{
  private let recorder: OrderingRecorder
  private let lock = NSLock()
  private var finishCount = 0
  private var errorCount = 0
  private var anchors: [NSFileProviderSyncAnchor] = []
  private var moreComing: [Bool] = []
  private var updates = 0
  private var recordedErrorCodes: [Int] = []

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

  func finishEnumeratingWithError(_ error: any Error) {
    lock.withLock {
      errorCount += 1
      recordedErrorCodes.append((error as NSError).code)
    }
    recorder.append("error")
  }

  func finishes() -> Int {
    lock.withLock { finishCount }
  }

  func errors() -> Int {
    lock.withLock { errorCount }
  }

  func errorCodes() -> [Int] {
    lock.withLock { recordedErrorCodes }
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

actor EnumeratorTransport: CatalogTransport {
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

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .catalogChangesSince:
      return try changes(payload: payload, decoder: decoder, encoder: encoder)
    case .activationAck:
      return try acknowledge(tenant: tenant, payload: payload, decoder: decoder, encoder: encoder)
    default:
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
  }

  private func changes(
    payload: Data,
    decoder: JSONDecoder,
    encoder: JSONEncoder
  ) throws -> Data {
    let request = try decoder.decode(CatalogChangesSinceRequest.self, from: payload)
    cursors.append("\(request.cursor.revision):\(request.cursor.sequence)")
    let partial = paginatedObject != nil && request.cursor.revision == 6
    let sequence: UInt32 = partial ? 1 : CatalogProtocol.changeCursorCompleteSequence
    let changes =
      paginatedObject.map {
        [CatalogChange(revision: 7, sequence: partial ? 1 : 2, kind: .upsert, object: $0)]
      } ?? []
    return try encoder.encode(
      CatalogChangesSinceResponse(
        code: .ok,
        message: "",
        floor: 1,
        head: 7,
        next: CatalogChangeCursor(revision: 7, sequence: sequence),
        complete: !partial,
        changes: changes
      )
    )
  }

  private func acknowledge(
    tenant: String,
    payload: Data,
    decoder: JSONDecoder,
    encoder: JSONEncoder
  ) throws -> Data {
    let request = try decoder.decode(CatalogAckActivationRequest.self, from: payload)
    recorder.append("ack")
    if failAcknowledgement {
      throw CatalogEnumeratorTestError.acknowledgement
    }
    acked.append(request.activationRevision)
    return try encoder.encode(
      CatalogAckActivationResponse(
        code: .ok,
        message: ""
      )
    )
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
