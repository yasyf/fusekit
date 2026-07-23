import DaemonKit
import Foundation

/// CatalogTransportError reports an exact persistent-session failure.
public enum CatalogTransportError: Error, Equatable, Sendable {
  case rejected(String)
  case remote(String)
  case missingPayload
  case bindingRequired
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

/// CatalogNotificationFeed pulls exact convergence events without an adapter buffer.
public struct CatalogNotificationFeed: Sendable {
  private let nextOperation: @Sendable () async throws -> CatalogConvergenceNotification?
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> CatalogConvergenceNotification?,
    cancel: @escaping @Sendable () async -> Void = {}
  ) {
    nextOperation = next
    cancelOperation = cancel
  }

  public func next() async throws -> CatalogConvergenceNotification? {
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
  func convergenceNotifications() -> CatalogNotificationFeed
}

/// SocketCatalogTransport carries catalog calls over one DaemonKit session.
public final class SocketCatalogTransport: CatalogTransport, @unchecked Sendable {
  private let connection: SocketCatalogConnection

  /// init validates the broker trust policy and endpoint without opening a session.
  public convenience init(
    appGroupEndpoint: CatalogAppGroupEndpoint,
    brokerTeamIdentifier: String,
    brokerSigningIdentifier: String,
    brokerRequiredEntitlements: [String: PeerTrust.EntitlementRequirement],
    configuration: SocketClient.Configuration = .init()
  ) throws {
    let requirement = try PeerTrust.Requirement(
      teamIdentifier: brokerTeamIdentifier,
      signingIdentifier: brokerSigningIdentifier,
      requiredAppGroup: appGroupEndpoint.identifier,
      requiredEntitlements: brokerRequiredEntitlements
    )
    try self.init(
      socketPath: appGroupEndpoint.socketPath(),
      configuration: configuration,
      trust: PeerTrust(requirement: requirement)
    )
  }

  init(socketPath: String, configuration: SocketClient.Configuration, trust: PeerTrust) {
    connection = SocketCatalogConnection(
      path: socketPath,
      configuration: configuration,
      trust: trust
    )
  }

  public func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let client = try await connection.client()
    return try await Self.payload(
      from: client.call(operation: operation.rawValue, tenant: tenant, payload: payload)
    )
  }

  public func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let payload = try encoder.encode(
      CatalogBrokerBindDomainRequest(
        domainID: domainID,
        tenantID: tenant.identifier,
        generation: tenant.generation
      )
    )
    let data = try await unary(
      operation: .brokerBindDomain,
      tenant: "",
      payload: payload
    )
    let response = try JSONDecoder().decode(CatalogBrokerBindDomainResponse.self, from: data)
    guard response.code == .ok else { throw CatalogTransportError.remote(response.message) }
  }

  public func download(operation: CatalogOperation, tenant: String, payload: Data) async throws
    -> CatalogDownload {
    let client = try await connection.client()
    let call = try await client.open(operation: operation.rawValue, tenant: tenant, payload: payload)
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
    let client = try await connection.client()
    let call = try await client.open(
      operation: operation.rawValue,
      tenant: tenant,
      payload: payload,
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

  public func convergenceNotifications() -> CatalogNotificationFeed {
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

private actor SocketCatalogConnection {
  private struct Session {
    let id: UInt64
    let task: Task<SocketClient, Error>
  }

  private let path: String
  private let configuration: SocketClient.Configuration
  private let trust: PeerTrust
  private var session: Session?
  private var nextSessionID: UInt64 = 1
  private var eventCursor: SocketEventCursor?

  init(path: String, configuration: SocketClient.Configuration, trust: PeerTrust) {
    self.path = path
    self.configuration = configuration
    self.trust = trust
  }

  func client() async throws -> SocketClient {
    let current: Session
    if let session {
      current = session
    } else {
      let path = path
      let configuration = configuration
      let trust = trust
      let id = nextSessionID
      nextSessionID += 1
      let task = Task {
        try await SocketClient(
          path: path,
          wireBuild: FuseKitTransportProtocol.wireBuild,
          configuration: configuration,
          trust: trust
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

  func nextNotification() async throws -> CatalogConvergenceNotification? {
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

  func nextNotification() async throws -> CatalogConvergenceNotification? {
    guard !reading else { throw CursorError.concurrentRead }
    reading = true
    defer { reading = false }
    while let event = try await events.nextEvent() {
      guard event.topic == CatalogOperation.convergenceNotify.rawValue else { continue }
      return try decoder.decode(CatalogConvergenceNotification.self, from: event.payload)
    }
    return nil
  }
}
