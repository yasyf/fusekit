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
  private var received: [CatalogAckActivationRequest] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    guard operation == .activationAck else {
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
    let request = try JSONDecoder().decode(CatalogAckActivationRequest.self, from: payload)
    received.append(request)
    return try JSONEncoder().encode(
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

  func acknowledgements() -> [CatalogAckActivationRequest] {
    received
  }
}

actor ScopeTransport: CatalogTransport {
  private var received: [String] = []

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
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

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
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

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation _: CatalogOperation, tenant _: String, payload _: Data) async throws -> Data
  {
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
