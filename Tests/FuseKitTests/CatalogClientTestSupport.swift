import Foundation
@testable import FuseKit

func testDomainID(
  owner: String = "owner-1",
  account: String = "account-1"
) throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID(owner),
    presentationInstanceID: CatalogPresentationInstanceID(account)
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

actor SnapshotTransport: CatalogTransport {
  private let response: CatalogSnapshotResponse

  init(response: CatalogSnapshotResponse) {
    self.response = response
  }

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload _: Data) async throws -> Data {
    guard operation == .catalogSnapshot else {
      throw CatalogTransportError.remote("unexpected operation")
    }
    return try JSONEncoder().encode(response)
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
}

actor OpenTransport: CatalogTransport {
  private let object: CatalogObject

  init(object: CatalogObject) {
    self.object = object
  }

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation _: CatalogOperation, tenant _: String, payload _: Data) async throws -> Data {
    throw CatalogTransportError.remote("unexpected unary operation")
  }

  func download(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    guard operation == .catalogOpenAt else {
      throw CatalogTransportError.remote("unexpected download")
    }
    let terminal = try JSONEncoder().encode(
      CatalogOpenAtResponse(code: .ok, message: "", object: object)
    )
    return CatalogDownload(
      next: { nil },
      terminal: { terminal },
      cancel: {}
    )
  }

  func upload(
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }
}

actor PreparationTransport: CatalogTransport {
  private var receivedTenant: CatalogPrepareTenantRequest?
  private var receivedDomain: CatalogPrepareDomainRequest?

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    let tenantID = try CatalogTenantID(tenant)
    let catalogRevision: UInt64 = 107
    switch operation {
    case .tenantPrepare:
      let request = try JSONDecoder().decode(CatalogPrepareTenantRequest.self, from: payload)
      receivedTenant = request
      return try tenantResponse(
        tenantID: tenantID,
        request: request,
        catalogRevision: catalogRevision
      )
    case .domainPrepare:
      let request = try JSONDecoder().decode(CatalogPrepareDomainRequest.self, from: payload)
      receivedDomain = request
      return try domainResponse(tenantID: tenantID, request: request)
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

  func tenantRequest() -> CatalogPrepareTenantRequest? {
    receivedTenant
  }

  func domainRequest() -> CatalogPrepareDomainRequest? {
    receivedDomain
  }

  private func tenantResponse(
    tenantID: CatalogTenantID,
    request: CatalogPrepareTenantRequest,
    catalogRevision: UInt64
  ) throws -> Data {
    let presentation: CatalogPresentationProof
    switch request.presentation {
    case .mount:
      presentation = CatalogPresentationProof(
        kind: .mount,
        mount: CatalogMountPresentationProof(
          tenantID: tenantID,
          generation: request.generation,
          publicPath: "/Volumes/FuseKit/\(tenantID.rawValue)",
          activationGeneration: request.activationGeneration
        )
      )
    case .fileProvider:
      let owner = try CatalogOwnerID("test-owner")
      let instance = try CatalogPresentationInstanceID("test-presentation")
      presentation = CatalogPresentationProof(
        kind: .fileProvider,
        fileProvider: CatalogFileProviderPresentationProof(
          tenantID: tenantID,
          domainID: CatalogDomainID.derived(ownerID: owner, presentationInstanceID: instance),
          generation: request.generation,
          publicPath: "/Library/CloudStorage/\(tenantID.rawValue)",
          activationGeneration: request.activationGeneration
        )
      )
    }
    return try JSONEncoder().encode(
      CatalogPrepareTenantResponse(
        code: .ok,
        message: "",
        proof: CatalogTenantPreparationProof(
          catalog: CatalogLaneProof(
            tenant: tenantID,
            generation: request.generation,
            requested: catalogRevision,
            desired: catalogRevision,
            observed: catalogRevision,
            verified: catalogRevision,
            applied: catalogRevision
          ),
          presentation: presentation,
          sourceAuthority: CatalogSourceAuthorityID("source-main"),
          sourceRevision: 7,
          catalogRevision: catalogRevision,
          changeID: CatalogChangeID("11111111111111111111111111111111"),
          operationID: CatalogOperationID("22222222222222222222222222222222")
        )
      )
    )
  }

  private func domainResponse(
    tenantID: CatalogTenantID,
    request: CatalogPrepareDomainRequest
  ) throws -> Data {
    try JSONEncoder().encode(
      CatalogPrepareDomainResponse(
        code: .ok,
        message: "",
        observation: CatalogDomainObservation(
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
  }
}
