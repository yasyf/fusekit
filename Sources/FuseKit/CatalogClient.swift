import Foundation

/// CatalogClientError reports a typed catalog contract failure.
public enum CatalogClientError: Error, Equatable, Sendable {
  case response(CatalogErrorCode, String)
  case missingObject
  case missingMutationIdentifier
  case mutationIdentityMismatch
  case invalidGeneration
}

/// CatalogTenant binds a transport tenant to one immutable catalog generation.
public struct CatalogTenant: Hashable, Sendable {
  public let identifier: CatalogTenantID
  public let generation: UInt64

  public init(identifier: CatalogTenantID, generation: UInt64) throws {
    guard generation > 0 else { throw CatalogClientError.invalidGeneration }
    self.identifier = identifier
    self.generation = generation
  }
}

/// CatalogContentDownload streams exact object bytes and verifies terminal metadata.
public struct CatalogContentDownload: Sendable {
  private let nextOperation: @Sendable () async throws -> Data?
  private let terminal: @Sendable () async throws -> CatalogObject
  private let cancelOperation: @Sendable () async -> Void

  public init(
    next: @escaping @Sendable () async throws -> Data?,
    terminal: @escaping @Sendable () async throws -> CatalogObject,
    cancel: @escaping @Sendable () async -> Void
  ) {
    nextOperation = next
    self.terminal = terminal
    cancelOperation = cancel
  }

  public func next() async throws -> Data? {
    try await nextOperation()
  }

  public func response() async throws -> CatalogObject {
    try await terminal()
  }

  public func cancel() async {
    await cancelOperation()
  }
}

/// CatalogClient exposes only exact catalog operations over a persistent session.
public struct CatalogClient: Sendable {
  private let transport: any CatalogTransport
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder

  public init(transport: any CatalogTransport) {
    self.transport = transport
    encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    decoder = JSONDecoder()
  }

  public func bind(domainID: CatalogDomainID, tenant: CatalogTenant) async throws {
    try await transport.bind(domainID: domainID, tenant: tenant)
  }

  public func head(tenant: CatalogTenant) async throws -> UInt64 {
    let response: CatalogHeadResponse = try await unary(
      operation: .catalogHead,
      tenant: tenant.identifier.rawValue,
      request: CatalogHeadRequest(generation: tenant.generation)
    )
    try Self.check(response.code, response.message)
    return response.revision
  }

  public func lookup(tenant: CatalogTenant, objectID: CatalogObjectID) async throws -> CatalogObject {
    let response: CatalogLookupResponse = try await unary(
      operation: .catalogLookup,
      tenant: tenant.identifier.rawValue,
      request: CatalogLookupRequest(generation: tenant.generation, objectID: objectID)
    )
    try Self.check(response.code, response.message)
    guard let object = response.object else { throw CatalogClientError.missingObject }
    return object
  }

  public func lookup(
    tenant: CatalogTenant,
    parentID: CatalogObjectID,
    name: String
  ) async throws -> CatalogObject {
    let response: CatalogLookupResponse = try await unary(
      operation: .catalogLookupName,
      tenant: tenant.identifier.rawValue,
      request: CatalogLookupNameRequest(
        generation: tenant.generation,
        parentID: parentID,
        name: name
      )
    )
    try Self.check(response.code, response.message)
    guard let object = response.object else { throw CatalogClientError.missingObject }
    return object
  }

  public func snapshot(
    tenant: CatalogTenant,
    revision: UInt64,
    scope: CatalogEnumerationScope,
    after: CatalogObjectID? = nil,
    limit: UInt32
  ) async throws -> CatalogSnapshotResponse {
    let response: CatalogSnapshotResponse = try await unary(
      operation: .catalogSnapshot,
      tenant: tenant.identifier.rawValue,
      request: CatalogSnapshotRequest(
        generation: tenant.generation,
        revision: revision,
        scope: scope,
        after: after,
        limit: limit
      )
    )
    try Self.check(response.code, response.message)
    guard response.revision == revision else {
      throw CatalogClientError.response(.integrity, "snapshot revision changed")
    }
    return response
  }

  public func changes(
    tenant: CatalogTenant,
    since cursor: CatalogChangeCursor,
    scope: CatalogEnumerationScope,
    limit: UInt32
  ) async throws -> CatalogChangesSinceResponse {
    let response: CatalogChangesSinceResponse = try await unary(
      operation: .catalogChangesSince,
      tenant: tenant.identifier.rawValue,
      request: CatalogChangesSinceRequest(
        generation: tenant.generation,
        cursor: cursor,
        scope: scope,
        limit: limit
      )
    )
    try Self.check(response.code, response.message)
    guard response.floor <= response.head,
          Self.cursor(response.next, isAfter: cursor) || Self.sameCursor(response.next, cursor),
          response.changes.allSatisfy({ $0.revision <= response.head })
    else {
      throw CatalogClientError.response(.integrity, "invalid change cursor range")
    }
    var previous = cursor
    for change in response.changes {
      let current = CatalogChangeCursor(revision: change.revision, sequence: change.sequence)
      guard change.sequence != CatalogProtocol.changeCursorCompleteSequence,
            Self.cursor(current, isAfter: previous)
      else {
        throw CatalogClientError.response(.integrity, "changes are not strictly ordered")
      }
      previous = current
    }
    if response.complete {
      guard response.next.revision == response.head,
            response.next.sequence == CatalogProtocol.changeCursorCompleteSequence
      else {
        throw CatalogClientError.response(.integrity, "complete response has partial cursor")
      }
    } else {
      guard let last = response.changes.last,
            response.next.revision == last.revision,
            response.next.sequence == last.sequence
      else {
        throw CatalogClientError.response(.integrity, "partial response has inexact cursor")
      }
    }
    return response
  }

  public func open(
    tenant: CatalogTenant,
    objectID: CatalogObjectID,
    revision: UInt64
  ) async throws -> CatalogContentDownload {
    let request = CatalogOpenAtRequest(
      generation: tenant.generation,
      objectID: objectID,
      revision: revision
    )
    let download = try await transport.download(
      operation: .catalogOpenAt,
      tenant: tenant.identifier.rawValue,
      payload: encoder.encode(request)
    )
    return CatalogContentDownload(
      next: { try await download.next() },
      terminal: {
        let response = try await decoder.decode(
          CatalogOpenAtResponse.self,
          from: download.response()
        )
        try Self.check(response.code, response.message)
        guard let object = response.object else { throw CatalogClientError.missingObject }
        return object
      },
      cancel: { await download.cancel() }
    )
  }

  public func mutate(
    tenant: CatalogTenant,
    request: CatalogMutationRequest,
    content: CatalogUpload = .empty
  ) async throws -> CatalogMutationResponse {
    let data = try await transport.upload(
      operation: .catalogMutate,
      tenant: tenant.identifier.rawValue,
      payload: encoder.encode(request),
      body: content
    )
    let response = try decoder.decode(CatalogMutationResponse.self, from: data)
    try Self.check(response.code, response.message)
    guard response.operationID == request.operationID else {
      throw CatalogClientError.mutationIdentityMismatch
    }
    return response
  }

  public func prepare(
    tenant: CatalogTenant,
    notification: CatalogConvergenceNotification
  ) async throws -> CatalogPreparationProof {
    let response: CatalogPrepareTenantResponse = try await unary(
      operation: .tenantPrepare,
      tenant: tenant.identifier.rawValue,
      request: CatalogPrepareTenantRequest(
        domainID: notification.domainID,
        generation: tenant.generation,
        catalogRevision: notification.catalogRevision,
        revision: notification.revision,
        sourceAuthority: notification.sourceAuthority,
        sourceRevision: notification.sourceRevision,
        changeID: notification.changeID,
        operationID: notification.operationID
      )
    )
    try Self.check(response.code, response.message)
    guard let proof = response.proof,
          Self.valid(proof.catalog, tenant: tenant, catalogRevision: notification.catalogRevision),
          Self.valid(proof.domain, tenant: tenant, notification: notification)
    else {
      throw CatalogClientError.response(.integrity, "missing proof")
    }
    return proof
  }

  public func acknowledge(
    tenant: CatalogTenant,
    notification: CatalogConvergenceNotification
  ) async throws -> CatalogDomainObservation {
    let response: CatalogAckConvergenceResponse = try await unary(
      operation: .convergenceAck,
      tenant: tenant.identifier.rawValue,
      request: CatalogAckConvergenceRequest(
        domainID: notification.domainID,
        generation: tenant.generation,
        revision: notification.revision,
        catalogRevision: notification.catalogRevision,
        sourceAuthority: notification.sourceAuthority,
        sourceRevision: notification.sourceRevision,
        changeID: notification.changeID,
        operationID: notification.operationID
      )
    )
    try Self.check(response.code, response.message)
    guard let observation = response.observation,
          Self.valid(observation, tenant: tenant, notification: notification)
    else {
      throw CatalogClientError.response(.integrity, "acknowledgement proof mismatch")
    }
    return observation
  }

  public func convergenceNotifications() -> CatalogNotificationFeed {
    transport.convergenceNotifications()
  }

  private func unary<Response: Decodable>(
    operation: CatalogOperation,
    tenant: String,
    request: some Encodable
  ) async throws -> Response {
    let data = try await transport.unary(
      operation: operation,
      tenant: tenant,
      payload: encoder.encode(request)
    )
    return try decoder.decode(Response.self, from: data)
  }

  private static func check(_ code: CatalogErrorCode, _ message: String) throws {
    guard code == .ok else { throw CatalogClientError.response(code, message) }
  }

  private static func cursor(
    _ lhs: CatalogChangeCursor,
    isAfter rhs: CatalogChangeCursor
  ) -> Bool {
    lhs.revision > rhs.revision
      || (lhs.revision == rhs.revision && lhs.sequence > rhs.sequence)
  }

  private static func sameCursor(_ lhs: CatalogChangeCursor, _ rhs: CatalogChangeCursor) -> Bool {
    lhs.revision == rhs.revision && lhs.sequence == rhs.sequence
  }

  private static func valid(
    _ proof: CatalogLaneProof,
    tenant: CatalogTenant,
    catalogRevision: UInt64
  ) -> Bool {
    proof.tenant == tenant.identifier
      && proof.generation == tenant.generation
      && proof.requested == catalogRevision
      && proof.desired >= catalogRevision
      && proof.observed >= catalogRevision
      && proof.verified >= catalogRevision
      && proof.applied >= catalogRevision
  }

  private static func valid(
    _ observation: CatalogDomainObservation,
    tenant: CatalogTenant,
    notification: CatalogConvergenceNotification
  ) -> Bool {
    observation.tenantID == tenant.identifier
      && observation.domainID == notification.domainID
      && observation.generation == tenant.generation
      && observation.requestedRevision == notification.revision
      && observation.observedRevision >= notification.revision
      && observation.catalogRevision == notification.catalogRevision
      && observation.sourceAuthority == notification.sourceAuthority
      && observation.sourceRevision == notification.sourceRevision
      && observation.changeID == notification.changeID
      && observation.operationID == notification.operationID
  }
}
