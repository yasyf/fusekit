import Foundation
@testable import FuseKit

func runtimeDomainID() throws -> CatalogDomainID {
  try CatalogDomainID.derived(
    ownerID: CatalogOwnerID("owner-1"),
    presentationInstanceID: CatalogPresentationInstanceID("account-1")
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
  private let recorder = CriticalFetchAckRecorder()
  private let ackError: CatalogTransportError?
  private let criticalContext: CatalogCriticalFetchContext?

  init(
    object: CatalogObject,
    source: DownloadSource,
    criticalContext: CatalogCriticalFetchContext? = nil,
    ackError: CatalogTransportError? = nil
  ) {
    self.object = object
    self.source = source
    self.criticalContext = criticalContext
    self.ackError = ackError
  }

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  func activationNotifications() -> CatalogNotificationFeed {
    .empty
  }

  func unary(operation: CatalogOperation, tenant: String, payload: Data) async throws -> Data {
    switch operation {
    case .catalogLookup:
      return try JSONEncoder().encode(
        CatalogLookupResponse(code: .ok, message: "", object: object)
      )
    case .criticalReadinessResolve:
      let request = try JSONDecoder().decode(CatalogResolveCriticalFetchRequest.self, from: payload)
      await recorder.recordResolve(tenant: tenant, request: request)
      return try JSONEncoder().encode(
        CatalogResolveCriticalFetchResponse(
          code: .ok,
          message: "",
          context: criticalContext
        )
      )
    case .criticalReadinessFetchAck:
      let request = try JSONDecoder().decode(CatalogAckCriticalFetchRequest.self, from: payload)
      await recorder.record(tenant: tenant, request: request)
      if let ackError {
        throw ackError
      }
      return try JSONEncoder().encode(
        CatalogAckCriticalFetchResponse(code: .ok, message: "")
      )
    default:
      throw CatalogTransportError.remote("unexpected operation \(operation.rawValue)")
    }
  }

  func criticalFetchAcks() async -> [CriticalFetchAckRecorder.Ack] {
    await recorder.recorded()
  }

  func criticalFetchResolves() async -> [CriticalFetchAckRecorder.Resolve] {
    await recorder.resolved()
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

actor CriticalFetchAckRecorder {
  struct Ack: Sendable {
    let tenant: String
    let request: CatalogAckCriticalFetchRequest
  }

  struct Resolve: Sendable {
    let tenant: String
    let request: CatalogResolveCriticalFetchRequest
  }

  private var acknowledgements: [Ack] = []
  private var resolutions: [Resolve] = []

  func record(tenant: String, request: CatalogAckCriticalFetchRequest) {
    acknowledgements.append(Ack(tenant: tenant, request: request))
  }

  func recordResolve(tenant: String, request: CatalogResolveCriticalFetchRequest) {
    resolutions.append(Resolve(tenant: tenant, request: request))
  }

  func recorded() -> [Ack] {
    acknowledgements
  }

  func resolved() -> [Resolve] {
    resolutions
  }
}

func criticalFetchContext() throws -> CatalogCriticalFetchContext {
  try CatalogCriticalFetchContext(
    leaseID: "lease-1",
    resolutionDigest: String(repeating: "2", count: 64),
    readChallenge: String(repeating: "5", count: 64)
  )
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

  nonisolated func activationNotifications() -> CatalogNotificationFeed {
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
        requestID: request.requestID,
        mutationID: CatalogMutationID(
          "0000000000000006222222222222222222222222222222222222222222222222"
        ),
        revision: 6,
        primaryID: request.objectID ?? source.id
      )
    )
  }

  func mutations() -> [Mutation] {
    recorded
  }
}
