import Foundation
@testable import FuseKit

func testDomainID(
  owner: String = "owner-1",
  account: String = "account-1"
) throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID(owner),
    accountInstanceID: CatalogAccountInstanceID(account)
  )
}

actor AckTransport: CatalogTransport {
  private var received: [CatalogAckConvergenceRequest] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    guard operation == .convergenceAck else {
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
    let request = try JSONDecoder().decode(CatalogAckConvergenceRequest.self, from: payload)
    received.append(request)
    return try JSONEncoder().encode(
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

  func acknowledgements() -> [CatalogAckConvergenceRequest] {
    received
  }
}

actor ScopeTransport: CatalogTransport {
  private var received: [String] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload: Data) async throws -> Data {
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .catalogSnapshot:
      let request = try decoder.decode(CatalogSnapshotRequest.self, from: payload)
      received.append(
        "snapshot:\(request.generation):\(request.scope.kind.rawValue):\(request.scope.parentID?.rawValue ?? "")"
      )
      return try encoder.encode(
        CatalogSnapshotResponse(
          code: .ok,
          message: "",
          revision: request.revision,
          objects: []
        )
      )
    case .catalogChangesSince:
      let request = try decoder.decode(CatalogChangesSinceRequest.self, from: payload)
      received.append(
        "changes:\(request.generation):\(request.scope.kind.rawValue):\(request.scope.parentID?.rawValue ?? "")"
      )
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
    default:
      throw CatalogTransportError.remote("unexpected operation")
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

  func scopes() -> [String] {
    received
  }
}

actor PreparationTransport: CatalogTransport {
  private var received: CatalogPrepareTenantRequest?

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    guard operation == .tenantPrepare else {
      throw CatalogTransportError.remote("unexpected operation")
    }
    let request = try JSONDecoder().decode(CatalogPrepareTenantRequest.self, from: payload)
    received = request
    let tenantID = try CatalogTenantID(tenant)
    return try JSONEncoder().encode(
      CatalogPrepareTenantResponse(
        code: .ok,
        message: "",
        proof: CatalogPreparationProof(
          catalog: CatalogLaneProof(
            tenant: tenantID,
            generation: request.generation,
            requested: request.catalogRevision,
            desired: request.catalogRevision,
            observed: request.catalogRevision,
            verified: request.catalogRevision,
            applied: request.catalogRevision
          ),
          domain: CatalogDomainObservation(
            tenantID: tenantID,
            domainID: request.domainID,
            generation: request.generation,
            requestedRevision: 7,
            observedRevision: 7,
            catalogRevision: request.catalogRevision,
            sourceAuthority: request.sourceAuthority,
            sourceRevision: request.sourceRevision,
            changeID: request.changeID,
            operationID: request.operationID
          )
        )
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

  func request() -> CatalogPrepareTenantRequest? {
    received
  }
}
