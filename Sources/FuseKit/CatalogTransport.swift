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
  private let client: SocketClient

  private init(client: SocketClient) {
    self.client = client
  }

  /// init authenticates the exact signed broker before sending transport bytes.
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
      client: SocketClient(
        path: appGroupEndpoint.socketPath(),
        build: FuseKitTransportProtocol.daemonkitBuild,
        configuration: configuration,
        trust: PeerTrust(requirement: requirement)
      )
    )
  }

  public func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data
  {
    try await Self.payload(
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
    -> CatalogDownload
  {
    let call = try client.open(operation: operation.rawValue, tenant: tenant, payload: payload)
    let cursor = SocketDownloadCursor(chunks: call.chunks)
    return CatalogDownload(
      next: { try await cursor.next() },
      terminal: { try await Self.payload(from: call.response()) },
      cancel: {
        call.cancel()
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
    let call = try client.open(
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
      call.cancel()
      _ = try? await call.response()
      throw error
    }
  }

  public func convergenceNotifications() -> CatalogNotificationFeed {
    let cursor = SocketEventCursor(events: client.events)
    return CatalogNotificationFeed(
      next: { try await cursor.nextNotification() },
      cancel: { self.client.close() }
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
