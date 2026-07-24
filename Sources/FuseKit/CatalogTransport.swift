import DaemonKit
import Foundation

/// CatalogTransportError reports an exact persistent-session failure.
public enum CatalogTransportError: Error, Equatable, Sendable {
  case rejected(String)
  case remote(String)
  case missingPayload
  case bindingRequired
  case bindingConflict
  case operationNotForwardable
}

/// CatalogDownload is one streamed response with a terminal JSON payload.
public struct CatalogDownload: Sendable {
  private let nextOperation: @Sendable () async throws -> Data?
  private let terminal: @Sendable () async throws -> Data
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> Data?,
    terminal: @escaping @Sendable () async throws -> Data,
    cancel: @escaping @Sendable () async -> Void
  ) {
    nextOperation = next
    self.terminal = terminal
    cancelOperation = cancel
  }

  public func next() async throws -> Data? {
    try await nextOperation()
  }

  public func response() async throws -> Data {
    try await terminal()
  }

  public func cancel() async {
    await cancelOperation()
  }
}

/// CatalogUpload is a pull-driven request body with no producer-side buffer.
public struct CatalogUpload: Sendable {
  private let nextOperation: @Sendable () async throws -> Data?
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> Data?,
    cancel: @escaping @Sendable () async -> Void = {}
  ) {
    nextOperation = next
    cancelOperation = cancel
  }

  public func next() async throws -> Data? {
    try await nextOperation()
  }

  public func cancel() async {
    await cancelOperation()
  }

  public static let empty = CatalogUpload(next: { nil })
}

/// CatalogNotificationFeed pulls exact activation events without an adapter buffer.
public struct CatalogNotificationFeed: Sendable {
  private let nextOperation: @Sendable () async throws -> CatalogActivationNotification?
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> CatalogActivationNotification?,
    cancel: @escaping @Sendable () async -> Void = {}
  ) {
    nextOperation = next
    cancelOperation = cancel
  }

  public func next() async throws -> CatalogActivationNotification? {
    try await nextOperation()
  }

  public func cancel() async {
    await cancelOperation()
  }

  public static let empty = CatalogNotificationFeed(next: { nil })
}

/// CatalogTransport is the byte-level seam used by the typed catalog client.
public protocol CatalogTransport: Sendable {
  func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws
  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data
  func download(operation: CatalogOperation, tenant: String, payload: Data) async throws
    -> CatalogDownload
  func upload(
    operation: CatalogOperation,
    tenant: String,
    payload: Data,
    body: CatalogUpload
  ) async throws -> Data
  func activationNotifications() -> CatalogNotificationFeed
}

/// SocketCatalogTransport carries catalog calls over one DaemonKit session.
public final class SocketCatalogTransport: CatalogTransport, @unchecked Sendable {
  private let connection: SocketCatalogConnection
  private let route = SocketCatalogRoute()

  /// init resolves the signed App Group endpoint without opening a session.
  public convenience init(
    appGroupEndpoint: CatalogAppGroupEndpoint,
    configuration: SocketClient.Configuration = .init()
  ) throws {
    try self.init(
      socketPath: appGroupEndpoint.socketPath(),
      configuration: configuration
    )
  }

  init(socketPath: String, configuration: SocketClient.Configuration) {
    connection = SocketCatalogConnection(
      path: socketPath,
      configuration: configuration
    )
  }

  public func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let forwarded = try await route.forward(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    return try await Self.payload(
      from: client.call(
        operation: CatalogOperation.brokerForward.rawValue,
        tenant: "",
        payload: forwarded
      )
    )
  }

  public func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    try await route.bind(domainID: domainID, tenant: tenant)
  }

  public func download(operation: CatalogOperation, tenant: String, payload: Data) async throws
    -> CatalogDownload {
    let forwarded = try await route.forward(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    let call = try await client.open(
      operation: CatalogOperation.brokerForward.rawValue,
      tenant: "",
      payload: forwarded
    )
    let cursor = SocketDownloadCursor(chunks: call.chunks)
    return CatalogDownload(
      next: { try await cursor.next() },
      terminal: { try await Self.payload(from: call.response()) },
      cancel: {
        await call.cancel()
        _ = try? await call.response()
      }
    )
  }

  public func upload(
    operation: CatalogOperation,
    tenant: String,
    payload: Data,
    body: CatalogUpload
  ) async throws -> Data {
    let forwarded = try await route.forward(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    let call = try await client.open(
      operation: CatalogOperation.brokerForward.rawValue,
      tenant: "",
      payload: forwarded,
      endInput: false
    )
    do {
      while let chunk = try await body.next() {
        try await call.sendChunk(chunk)
      }
      try await call.closeSend()
      return try await Self.payload(from: call.response())
    } catch {
      await body.cancel()
      await call.cancel()
      _ = try? await call.response()
      throw error
    }
  }

  public func activationNotifications() -> CatalogNotificationFeed {
    CatalogNotificationFeed(
      next: { try await self.connection.nextNotification() },
      cancel: { await self.connection.close() }
    )
  }

  private static func payload(from result: SocketTerminal) throws -> Data {
    if result.rejected {
      throw CatalogTransportError.rejected(result.reason ?? "request rejected")
    }
    if let error = result.error {
      throw CatalogTransportError.remote(error)
    }
    guard let payload = result.payload else {
      throw CatalogTransportError.missingPayload
    }
    return payload
  }
}

actor SocketCatalogRoute {
  private var context: CatalogBrokerForwardContext?
  private let encoder: JSONEncoder

  init() {
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  func bind(domainID: CatalogDomainID, tenant: CatalogTenant) throws {
    let proposed = CatalogBrokerForwardContext(
      domainID: domainID,
      tenantID: tenant.identifier,
      generation: tenant.generation
    )
    if let context {
      guard context.domainID == proposed.domainID,
            context.tenantID == proposed.tenantID,
            context.generation == proposed.generation
      else { throw CatalogTransportError.bindingConflict }
      return
    }
    context = proposed
  }

  func forward(operation: CatalogOperation, tenant: String, payload: Data) throws -> Data {
    guard let context else { throw CatalogTransportError.bindingRequired }
    guard tenant == context.tenantID.rawValue else {
      throw CatalogTransportError.bindingConflict
    }
    switch operation {
    case .catalogHead,
         .catalogSnapshot,
         .catalogChangesSince,
         .catalogLookup,
         .catalogLookupName,
         .catalogOpenAt,
         .catalogMutate,
         .activationAck,
         .criticalReadinessResolve,
         .criticalReadinessFetchAck,
         .materializationSnapshotBegin,
         .materializationSnapshotSuspend,
         .materializationSnapshotStagePage,
         .materializationSnapshotCommit:
      break
    default:
      throw CatalogTransportError.operationNotForwardable
    }
    return try encoder.encode(
      CatalogBrokerForwardRequest(
        context: context,
        operation: operation,
        payload: payload
      )
    )
  }
}

private actor SocketCatalogConnection {
  private struct Session {
    let id: UInt64
    let task: Task<SocketClient, Error>
  }

  private let path: String
  private let configuration: SocketClient.Configuration
  private var session: Session?
  private var nextSessionID: UInt64 = 1
  private var eventCursor: SocketEventCursor?

  init(path: String, configuration: SocketClient.Configuration) {
    self.path = path
    self.configuration = configuration
  }

  func client() async throws -> SocketClient {
    let current: Session
    if let session {
      current = session
    } else {
      let path = path
      let configuration = configuration
      let id = nextSessionID
      nextSessionID += 1
      let task = Task {
        try await SocketClient(
          path: path,
          wireBuild: FuseKitTransportProtocol.wireBuild,
          role: FuseKitSessionPeerRole.fileProviderExtension,
          configuration: configuration
        )
      }
      current = Session(id: id, task: task)
      session = current
    }

    do {
      return try await current.task.value
    } catch {
      if session?.id == current.id {
        session = nil
        eventCursor = nil
      }
      throw error
    }
  }

  func nextNotification() async throws -> CatalogActivationNotification? {
    if let eventCursor {
      return try await eventCursor.nextNotification()
    }
    let client = try await client()
    if eventCursor == nil {
      eventCursor = SocketEventCursor(events: client.events)
    }
    return try await eventCursor?.nextNotification()
  }

  func close() async {
    let current = session
    session = nil
    eventCursor = nil
    guard let current else { return }
    if let client = try? await current.task.value {
      await client.close()
    }
  }
}

private actor SocketDownloadCursor {
  private enum CursorError: Error { case concurrentRead }
  private let chunks: SocketChunkStream
  private var reading = false

  init(chunks: SocketChunkStream) {
    self.chunks = chunks
  }

  func next() async throws -> Data? {
    guard !reading else { throw CursorError.concurrentRead }
    reading = true
    defer { reading = false }
    while let chunk = try await chunks.nextChunk() {
      if !chunk.end {
        return chunk.payload
      }
    }
    return nil
  }
}

private actor SocketEventCursor {
  private enum CursorError: Error { case concurrentRead }
  private let events: SocketEventStream
  private let decoder = JSONDecoder()
  private var reading = false

  init(events: SocketEventStream) {
    self.events = events
  }

  func nextNotification() async throws -> CatalogActivationNotification? {
    guard !reading else { throw CursorError.concurrentRead }
    reading = true
    defer { reading = false }
    while let event = try await events.nextEvent() {
      guard event.topic == CatalogOperation.activationNotify.rawValue else { continue }
      return try decoder.decode(CatalogActivationNotification.self, from: event.payload)
    }
    return nil
  }
}
