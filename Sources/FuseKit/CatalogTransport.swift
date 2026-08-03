import CryptoKit
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

/// v0.21 reserves the wire tenant header, so the tenant rides this fusekit-owned
/// envelope; the already-encoded payload embeds verbatim as raw JSON — never a
/// re-encoded Codable field, which would base64 it and break the server decode.
enum CatalogRequestEnvelope {
  static func encode(tenant: String, payload: Data) throws -> Data {
    var body = Data(#"{"tenant":"#.utf8)
    try body.append(jsonString(tenant))
    body.append(Data(#","payload":"#.utf8))
    body.append(payload)
    body.append(Data("}".utf8))
    return body
  }

  private static func jsonString(_ value: String) throws -> Data {
    let quoted = try JSONEncoder().encode([value])
    return quoted.dropFirst().dropLast()
  }
}

struct CatalogRouteContext: Sendable {
  let domainID: CatalogDomainID
  let tenantID: CatalogTenantID
  let generation: UInt64
}

/// SocketCatalogTransport carries catalog calls over one DaemonKit session.
public final class SocketCatalogTransport: CatalogTransport, @unchecked Sendable {
  private let connection: SocketCatalogConnection
  private let route = SocketCatalogRoute()
  private let encoder: JSONEncoder
  private let decoder = JSONDecoder()

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
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  public func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let body = try await route.body(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    return try await catalogTerminalPayload(
      from: client.call(operation: operation.rawValue, payload: body)
    )
  }

  public func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    try await route.bind(domainID: domainID, tenant: tenant)
  }

  public func download(operation: CatalogOperation, tenant: String, payload: Data) async throws
    -> CatalogDownload {
    let body = try await route.body(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    let opened = try await catalogTerminalPayload(from: client.call(operation: operation.rawValue, payload: body))
    guard let handle = try Self.openHandle(from: opened, operation: operation) else {
      return CatalogDownload(next: { nil }, terminal: { opened }, cancel: {})
    }
    let reader = SocketPinnedReader(connection: connection, handle: handle)
    return CatalogDownload(
      next: { try await reader.next() },
      terminal: { opened },
      cancel: { await reader.cancel() }
    )
  }

  public func upload(
    operation: CatalogOperation,
    tenant: String,
    payload: Data,
    body: CatalogUpload
  ) async throws -> Data {
    let request = try decoder.decode(CatalogMutationRequest.self, from: payload)
    let beginBody = try await route.body(operation: operation, tenant: tenant, payload: payload)
    let client = try await connection.client()
    let begun = try await catalogTerminalPayload(from: client.call(operation: operation.rawValue, payload: beginBody))
    guard request.hasContent else {
      return begun
    }
    let begin = try decoder.decode(CatalogBeginMutationResponse.self, from: begun)
    guard begin.code == .ok else {
      throw CatalogTransportError.remote(begin.message)
    }
    var hasher = SHA256()
    var total: UInt64 = 0
    var sequence: UInt32 = 0
    do {
      while let chunk = try await body.next() {
        guard !chunk.isEmpty else { continue }
        sequence += 1
        total += UInt64(chunk.count)
        hasher.update(data: chunk)
        let chunked = try encoder.encode(
          CatalogMutationChunkRequest(requestID: request.requestID, sequence: sequence, payload: chunk)
        )
        let response = try await decoder.decode(
          CatalogMutationChunkResponse.self,
          from: catalogTerminalPayload(
            from: client.call(
              operation: CatalogOperation.catalogMutateChunk.rawValue,
              payload: CatalogRequestEnvelope.encode(tenant: "", payload: chunked)
            )
          )
        )
        guard response.code == .ok else { throw CatalogTransportError.remote(response.message) }
      }
    } catch {
      await body.cancel()
      throw error
    }
    guard total > 0 else {
      throw CatalogTransportError.remote("catalog service: content mutation streamed no bytes")
    }
    let digest = hasher.finalize().map { String(format: "%02x", $0) }.joined()
    let commit = try encoder.encode(
      CatalogCommitMutationRequest(requestID: request.requestID, total: total, digest: digest)
    )
    return try await catalogTerminalPayload(
      from: client.call(
        operation: CatalogOperation.catalogMutateCommit.rawValue,
        payload: CatalogRequestEnvelope.encode(tenant: "", payload: commit)
      )
    )
  }

  public func activationNotifications() -> CatalogNotificationFeed {
    let poller = SocketActivationPoller(connection: connection, route: route)
    return CatalogNotificationFeed(
      next: { try await poller.next() },
      cancel: { await poller.cancel() }
    )
  }

  private static func openHandle(
    from payload: Data,
    operation: CatalogOperation
  ) throws -> CatalogHandleID? {
    let decoder = JSONDecoder()
    switch operation {
    case .catalogOpenPrivate:
      return try decoder.decode(CatalogOpenPrivateResponse.self, from: payload).handle
    default:
      return try decoder.decode(CatalogOpenAtResponse.self, from: payload).handle
    }
  }
}

private func catalogTerminalPayload(from result: SocketTerminal) throws -> Data {
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

actor SocketCatalogRoute {
  private var context: CatalogRouteContext?

  func bind(domainID: CatalogDomainID, tenant: CatalogTenant) throws {
    let proposed = CatalogRouteContext(
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

  func snapshot() -> CatalogRouteContext? {
    context
  }

  func body(operation: CatalogOperation, tenant: String, payload: Data) throws -> Data {
    guard let context else { throw CatalogTransportError.bindingRequired }
    guard tenant == context.tenantID.rawValue else {
      throw CatalogTransportError.bindingConflict
    }
    switch operation {
    case .catalogHead,
         .catalogSnapshot,
         .catalogChangesSince,
         .catalogLookup,
         .catalogLookupPrivate,
         .catalogLookupName,
         .catalogOpenAt,
         .catalogOpenPrivate,
         .catalogMutateBegin,
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
    return try CatalogRequestEnvelope.encode(tenant: tenant, payload: payload)
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
          schema: FuseKitTransportProtocol.wireBuild,
          lane: .business,
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
      }
      throw error
    }
  }

  func close() async {
    let current = session
    session = nil
    guard let current else { return }
    if let client = try? await current.task.value {
      await client.close()
    }
  }
}

private actor SocketPinnedReader {
  private let connection: SocketCatalogConnection
  private let handle: CatalogHandleID
  private let encoder: JSONEncoder
  private let decoder = JSONDecoder()
  private var offset: UInt64 = 0
  private var eof = false
  private var closed = false
  private var read: Task<SocketTerminal, Error>?

  init(connection: SocketCatalogConnection, handle: CatalogHandleID) {
    self.connection = connection
    self.handle = handle
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  func next() async throws -> Data? {
    while true {
      if closed || eof {
        return nil
      }
      let request = try CatalogReadRequest(
        handle: handle, offset: offset, limit: CatalogProtocol.maxReadChunkBytes
      )
      let body = try CatalogRequestEnvelope.encode(tenant: "", payload: encoder.encode(request))
      let client = try await connection.client()
      let task = Task { try await client.call(operation: CatalogOperation.catalogRead.rawValue, payload: body) }
      read = task
      let terminal: SocketTerminal
      do {
        terminal = try await task.value
      } catch {
        read = nil
        if closed {
          return nil
        }
        throw error
      }
      read = nil
      if closed {
        return nil
      }
      let response = try decoder.decode(
        CatalogReadResponse.self, from: catalogTerminalPayload(from: terminal)
      )
      guard response.code == .ok else { throw CatalogTransportError.remote(response.message) }
      offset += UInt64(response.data.count)
      eof = response.eof
      if !response.data.isEmpty {
        return response.data
      }
      if eof {
        return nil
      }
    }
  }

  func cancel() async {
    if closed {
      return
    }
    closed = true
    read?.cancel()
    let request = CatalogCloseRequest(handle: handle)
    guard let body = try? encoder.encode(request),
          let client = try? await connection.client()
    else { return }
    _ = try? await client.call(
      operation: CatalogOperation.catalogClose.rawValue,
      payload: CatalogRequestEnvelope.encode(tenant: "", payload: body)
    )
  }
}

private actor SocketActivationPoller {
  private let connection: SocketCatalogConnection
  private let route: SocketCatalogRoute
  private let encoder: JSONEncoder
  private let decoder = JSONDecoder()
  private var cursor: UInt64 = 0
  private var buffer: [CatalogActivationNotification] = []
  private var index = 0
  private var closed = false
  private var poll: Task<SocketTerminal, Error>?

  init(connection: SocketCatalogConnection, route: SocketCatalogRoute) {
    self.connection = connection
    self.route = route
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
  }

  func next() async throws -> CatalogActivationNotification? {
    while true {
      if closed {
        return nil
      }
      if index < buffer.count {
        let notification = buffer[index]
        index += 1
        return notification
      }
      guard let context = await route.snapshot() else {
        throw CatalogTransportError.bindingRequired
      }
      let request = try CatalogPollActivationsRequest(
        domainID: context.domainID,
        generation: context.generation,
        cursor: cursor,
        waitMillis: CatalogProtocol.maxPollWaitMillis,
        limit: CatalogProtocol.maxActivationPollNotifications
      )
      let body = try CatalogRequestEnvelope.encode(
        tenant: context.tenantID.rawValue, payload: encoder.encode(request)
      )
      let client = try await connection.client()
      let task = Task { try await client.call(operation: CatalogOperation.activationPoll.rawValue, payload: body) }
      poll = task
      let terminal: SocketTerminal
      do {
        terminal = try await task.value
      } catch {
        poll = nil
        if closed {
          return nil
        }
        throw error
      }
      poll = nil
      if closed {
        return nil
      }
      let response = try decoder.decode(
        CatalogPollActivationsResponse.self, from: catalogTerminalPayload(from: terminal)
      )
      guard response.code == .ok else { throw CatalogTransportError.remote(response.message) }
      if !response.notifications.isEmpty {
        buffer = response.notifications
        index = 0
        cursor = response.nextCursor
      }
    }
  }

  func cancel() async {
    if closed {
      return
    }
    closed = true
    poll?.cancel()
    await connection.close()
  }
}
