import Foundation

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
    guard response.objects.count <= Int(limit),
          response.objects.allSatisfy({ !$0.tombstone && $0.revision <= revision })
    else {
      throw CatalogClientError.response(.integrity, "invalid snapshot page")
    }
    var previous = after?.rawValue
    for object in response.objects {
      guard previous == nil || object.id.rawValue > previous! else {
        throw CatalogClientError.response(.integrity, "snapshot objects are not strictly ordered")
      }
      previous = object.id.rawValue
    }
    if let next = response.next {
      guard let last = response.objects.last, next == last.id else {
        throw CatalogClientError.response(.integrity, "snapshot cursor does not match last object")
      }
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
}

extension CatalogClient {
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
        guard object.id == objectID,
              object.revision == revision,
              object.kind == .file,
              !object.tombstone
        else {
          throw CatalogClientError.response(.integrity, "stream metadata does not match request")
        }
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
    guard response.requestID == request.requestID, response.mutationID != nil else {
      throw CatalogClientError.mutationIdentityMismatch
    }
    return response
  }

  func acknowledgeCriticalFetch(
    tenant: CatalogTenant,
    object: CatalogObject,
    readHash: String
  ) async throws {
    let response: CatalogAckCriticalFetchResponse = try await unary(
      operation: .criticalReadinessFetchAck,
      tenant: tenant.identifier.rawValue,
      request: CatalogAckCriticalFetchRequest(
        generation: tenant.generation,
        objectID: object.id,
        objectRevision: object.revision,
        contentRevision: object.contentRevision,
        size: object.size,
        hash: object.hash,
        readHash: readHash
      )
    )
    try Self.check(response.code, response.message)
  }

  /// prepareTenant converges authoritative source, catalog, and one exact presentation.
  public func prepareTenant(
    tenant: CatalogTenant,
    presentation: CatalogPresentationKind,
    activationGeneration: String,
    criticalPolicyDigest: String,
    criticalObjects: [CatalogCriticalObjectRequirement],
    leaseID: String,
    leaseExpiresUnixNano: UInt64
  ) async throws -> CatalogTenantPreparationProof {
    let response: CatalogPrepareTenantResponse = try await unary(
      operation: .tenantPrepare,
      tenant: tenant.identifier.rawValue,
      request: CatalogPrepareTenantRequest(
        generation: tenant.generation,
        presentation: presentation,
        activationGeneration: activationGeneration,
        criticalPolicyDigest: criticalPolicyDigest,
        criticalObjects: criticalObjects,
        leaseID: leaseID,
        leaseExpiresUnixNano: leaseExpiresUnixNano
      )
    )
    try Self.check(response.code, response.message)
    guard let proof = response.proof,
          Self.valid(proof.catalog, tenant: tenant),
          proof.catalog.requested == proof.catalogRevision,
          Self.valid(
            proof.presentation,
            kind: presentation,
            tenant: tenant,
            activationGeneration: activationGeneration
          ),
          !proof.sourceAuthority.rawValue.isEmpty,
          !proof.sourcePublication.rawValue.isEmpty,
          proof.sourceRevision > 0,
          !proof.changeID.rawValue.isEmpty,
          !proof.operationID.rawValue.isEmpty
    else {
      throw CatalogClientError.response(.integrity, "missing tenant preparation proof")
    }
    return proof
  }

  private static func valid(
    _ proof: CatalogPresentationProof,
    kind: CatalogPresentationKind,
    tenant: CatalogTenant,
    activationGeneration: String
  ) -> Bool {
    guard proof.kind == kind, !activationGeneration.isEmpty else { return false }
    switch kind {
    case .mount:
      guard let mount = proof.mount, proof.fileProvider == nil else { return false }
      return mount.tenantID == tenant.identifier && mount.generation == tenant.generation
        && mount.activationGeneration == activationGeneration && exactAbsolutePath(mount.publicPath)
    case .fileProvider:
      guard let fileProvider = proof.fileProvider, proof.mount == nil else { return false }
      return fileProvider.tenantID == tenant.identifier
        && fileProvider.generation == tenant.generation
        && fileProvider.activationGeneration == activationGeneration
        && exactAbsolutePath(fileProvider.publicPath)
    }
  }

  private static func exactAbsolutePath(_ path: String) -> Bool {
    path.hasPrefix("/") && !path.contains("\0")
      && URL(fileURLWithPath: path).standardizedFileURL.path == path
  }

  public func acknowledge(
    tenant: CatalogTenant,
    notification: CatalogActivationNotification
  ) async throws {
    let response: CatalogAckActivationResponse = try await unary(
      operation: .activationAck,
      tenant: tenant.identifier.rawValue,
      request: CatalogAckActivationRequest(
        activationChangeID: notification.activationChangeID,
        domainID: notification.domainID,
        generation: tenant.generation,
        activationRevision: notification.activationRevision,
        catalogHead: notification.catalogHead,
        headDigest: notification.headDigest
      )
    )
    try Self.check(response.code, response.message)
  }

  public func activationNotifications() -> CatalogNotificationFeed {
    transport.activationNotifications()
  }

  func beginMaterializationSnapshot(
    binding: CatalogFileProviderBinding,
    snapshotID: CatalogMaterializationSnapshotID,
    backingStoreIdentity: Data
  ) async throws -> UInt64 {
    let response: CatalogBeginMaterializationSnapshotResponse = try await unary(
      operation: .materializationSnapshotBegin,
      tenant: binding.tenant.identifier.rawValue,
      request: CatalogBeginMaterializationSnapshotRequest(
        tenantID: binding.tenant.identifier,
        domainID: binding.domainID,
        generation: binding.tenant.generation,
        snapshotID: snapshotID,
        backingStoreIdentity: backingStoreIdentity
      )
    )
    try Self.check(response.code, response.message)
    return response.epoch
  }

  func suspendMaterialization(binding: CatalogFileProviderBinding) async throws {
    let response: CatalogSuspendMaterializationSnapshotResponse = try await unary(
      operation: .materializationSnapshotSuspend,
      tenant: binding.tenant.identifier.rawValue,
      request: CatalogSuspendMaterializationSnapshotRequest(
        tenantID: binding.tenant.identifier,
        domainID: binding.domainID,
        generation: binding.tenant.generation
      )
    )
    try Self.check(response.code, response.message)
  }

  func stageMaterializationPage(
    binding: CatalogFileProviderBinding,
    snapshotID: CatalogMaterializationSnapshotID,
    backingStoreIdentity: Data,
    sequence: UInt32,
    containerIDs: [CatalogObjectID]
  ) async throws {
    let response: CatalogStageMaterializationSnapshotPageResponse = try await unary(
      operation: .materializationSnapshotStagePage,
      tenant: binding.tenant.identifier.rawValue,
      request: CatalogStageMaterializationSnapshotPageRequest(
        tenantID: binding.tenant.identifier,
        domainID: binding.domainID,
        generation: binding.tenant.generation,
        snapshotID: snapshotID,
        backingStoreIdentity: backingStoreIdentity,
        sequence: sequence,
        containerIDs: containerIDs
      )
    )
    try Self.check(response.code, response.message)
  }

  func commitMaterializationSnapshot(
    binding: CatalogFileProviderBinding,
    snapshotID: CatalogMaterializationSnapshotID,
    backingStoreIdentity: Data,
    pageCount: UInt32
  ) async throws -> CatalogCommitMaterializationSnapshotResponse {
    let response: CatalogCommitMaterializationSnapshotResponse = try await unary(
      operation: .materializationSnapshotCommit,
      tenant: binding.tenant.identifier.rawValue,
      request: CatalogCommitMaterializationSnapshotRequest(
        tenantID: binding.tenant.identifier,
        domainID: binding.domainID,
        generation: binding.tenant.generation,
        snapshotID: snapshotID,
        backingStoreIdentity: backingStoreIdentity,
        pageCount: pageCount
      )
    )
    try Self.check(response.code, response.message)
    return response
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
    tenant: CatalogTenant
  ) -> Bool {
    proof.tenant == tenant.identifier
      && proof.generation == tenant.generation
      && proof.requested > 0
      && proof.desired == proof.requested
      && proof.observed == proof.requested
      && proof.verified == proof.requested
      && proof.applied == proof.requested
  }

}
