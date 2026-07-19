import Foundation
@testable import FuseKit

func runtimeDomainID() throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID("owner-1"),
    accountInstanceID: CatalogAccountInstanceID("account-1")
  )
}

enum DownloadTestError: Error, Equatable {
  case interrupted
}

actor DownloadSource {
  private let chunks: [Data]
  private let failureAt: Int?
  private var pulls = 0
  private var canceled = false

  init(
    chunks: [Data] = [
      Data(repeating: 1, count: 1024 * 1024),
      Data(repeating: 2, count: 1024 * 1024),
    ],
    failureAt: Int? = 3
  ) {
    self.chunks = chunks
    self.failureAt = failureAt
  }

  func next() async throws -> Data? {
    pulls += 1
    try await Task.sleep(for: .milliseconds(1))
    if let failureAt, pulls == failureAt {
      throw DownloadTestError.interrupted
    }
    guard pulls <= chunks.count else { return nil }
    return chunks[pulls - 1]
  }

  func cancel() {
    canceled = true
  }

  func pullCount() -> Int {
    pulls
  }

  func wasCanceled() -> Bool {
    canceled
  }
}

final class DownloadTransport: CatalogTransport, @unchecked Sendable {
  private let object: CatalogObject
  private let source: DownloadSource

  init(object: CatalogObject, source: DownloadSource) {
    self.object = object
    self.source = source
  }

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload _: Data) async throws -> Data {
    guard operation == .catalogLookup else {
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
    return try JSONEncoder().encode(
      CatalogLookupResponse(code: .ok, message: "", object: object)
    )
  }

  func download(
    operation: CatalogOperation,
    tenant _: String,
    payload _: Data
  ) async throws -> CatalogDownload {
    guard operation == .catalogOpenAt else {
      throw CatalogTransportError.remote("unexpected download")
    }
    return CatalogDownload(
      next: { try await self.source.next() },
      terminal: {
        try JSONEncoder().encode(
          CatalogOpenAtResponse(code: .ok, message: "", object: self.object)
        )
      },
      cancel: { await self.source.cancel() }
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

actor MutationTransport: CatalogTransport {
  struct Mutation: Sendable {
    let request: CatalogMutationRequest
    let content: Data
    let chunkSizes: [Int]
  }

  private let source: CatalogObject
  private let target: CatalogObject
  private var recorded: [Mutation] = []

  init(source: CatalogObject, target: CatalogObject) {
    self.source = source
    self.target = target
  }

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func convergenceNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant _: String, payload: Data) async throws -> Data {
    let encoder = JSONEncoder()
    let decoder = JSONDecoder()
    switch operation {
    case .catalogHead:
      guard try decoder.decode(CatalogHeadRequest.self, from: payload).generation == 4 else {
        throw CatalogTransportError.remote("wrong generation")
      }
      return try encoder.encode(CatalogHeadResponse(code: .ok, message: "", revision: 5))
    case .catalogLookup:
      guard try decoder.decode(CatalogLookupRequest.self, from: payload).generation == 4 else {
        throw CatalogTransportError.remote("wrong generation")
      }
      return try encoder.encode(CatalogLookupResponse(code: .ok, message: "", object: source))
    case .catalogLookupName:
      guard try decoder.decode(CatalogLookupNameRequest.self, from: payload).generation == 4 else {
        throw CatalogTransportError.remote("wrong generation")
      }
      return try encoder.encode(CatalogLookupResponse(code: .ok, message: "", object: target))
    default:
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
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
    operation: CatalogOperation,
    tenant _: String,
    payload: Data,
    body: CatalogUpload
  ) async throws -> Data {
    guard operation == .catalogMutate else {
      throw CatalogTransportError.remote("unexpected upload")
    }
    let request = try JSONDecoder().decode(CatalogMutationRequest.self, from: payload)
    var content = Data()
    var chunkSizes: [Int] = []
    while let chunk = try await body.next() {
      content.append(chunk)
      chunkSizes.append(chunk.count)
    }
    recorded.append(Mutation(request: request, content: content, chunkSizes: chunkSizes))
    return try JSONEncoder().encode(
      CatalogMutationResponse(
        code: .ok,
        message: "",
        operationID: request.operationID,
        revision: 6,
        primaryID: request.objectID ?? source.id
      )
    )
  }

  func mutations() -> [Mutation] {
    recorded
  }
}
