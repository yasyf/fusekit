import FileProvider
import Foundation
import Testing

@testable import FuseKit

@Suite("Authoritative File Provider materialization")
struct FileProviderMaterializationTests {
  @Test
  func startupPublishesOneCanonicalCompleteSnapshot() async throws {
    let transport = MaterializationTransport()
    let binding = try materializationBinding()
    let source = FixedMaterializedSetSource(
      identity: Data("store-1".utf8),
      identifiers: [
        NSFileProviderItemIdentifier(materializedObjectTwo.rawValue),
        .rootContainer,
        NSFileProviderItemIdentifier(materializedObjectOne.rawValue),
        NSFileProviderItemIdentifier(materializedObjectTwo.rawValue),
      ]
    )
    let coordinator = CatalogMaterializationCoordinator(
      binding: binding,
      client: CatalogClient(transport: transport),
      bindingGate: CatalogBindingGate(binding: binding, client: CatalogClient(transport: transport)),
      source: source
    )
    defer { coordinator.invalidate() }

    coordinator.markDirty()

    #expect(await eventually { await transport.commitCount() == 1 })
    #expect(
      await transport.stagedIDs() == [
        materializedRoot.rawValue,
        materializedObjectOne.rawValue,
        materializedObjectTwo.rawValue,
      ].sorted()
    )
    #expect(await transport.beginCount() == 1)
    #expect(await transport.suspendCount() == 0)
  }

  @Test
  func unavailableBackingStoreSuspendsWithoutReplacingLastGoodSet() async throws {
    let transport = MaterializationTransport()
    let binding = try materializationBinding()
    let client = CatalogClient(transport: transport)
    let coordinator = CatalogMaterializationCoordinator(
      binding: binding,
      client: client,
      bindingGate: CatalogBindingGate(binding: binding, client: client),
      source: FixedMaterializedSetSource(identity: nil, identifiers: [])
    )
    defer { coordinator.invalidate() }

    coordinator.markDirty()

    #expect(await eventually { await transport.suspendCount() == 1 })
    #expect(await transport.beginCount() == 0)
    #expect(await transport.commitCount() == 0)
  }

  @Test
  func partialEnumerationNeverCommitsOrClearsTheLastGoodSet() async throws {
    let transport = MaterializationTransport()
    let binding = try materializationBinding()
    let client = CatalogClient(transport: transport)
    let coordinator = CatalogMaterializationCoordinator(
      binding: binding,
      client: client,
      bindingGate: CatalogBindingGate(binding: binding, client: client),
      source: FailingMaterializedSetSource(identity: Data("store-1".utf8))
    )
    defer { coordinator.invalidate() }

    coordinator.markDirty()

    #expect(await eventually { await transport.beginCount() == 1 })
    #expect(await transport.stageCount() == 0)
    #expect(await transport.commitCount() == 0)
    #expect(await transport.suspendCount() == 0)
  }

  @Test
  func newerDirtyGenerationSupersedesACollectingSnapshot() async throws {
    let transport = MaterializationTransport()
    let binding = try materializationBinding()
    let client = CatalogClient(transport: transport)
    let source = BlockingMaterializedSetSource(identity: Data("store-1".utf8))
    let coordinator = CatalogMaterializationCoordinator(
      binding: binding,
      client: client,
      bindingGate: CatalogBindingGate(binding: binding, client: client),
      source: source
    )
    defer { coordinator.invalidate() }

    coordinator.markDirty()
    #expect(await eventually { await source.isBlocked() })
    coordinator.markDirty()
    await source.release()

    #expect(await eventually { await transport.commitCount() == 1 })
    #expect(await transport.beginCount() == 2)
    #expect(await transport.stageCount() == 1)
    #expect(await transport.stagedIDs() == [materializedObjectTwo.rawValue])
  }

  @Test
  func socketRouteWrapsEveryCallInTheExactBoundContext() async throws {
    let route = SocketCatalogRoute()
    let tenant = try CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4)
    let domain = try testDomainID()
    let payload = Data("payload".utf8)

    await #expect(throws: CatalogTransportError.bindingRequired) {
      try await route.forward(operation: .catalogHead, tenant: "tenant-1", payload: payload)
    }
    try await route.bind(domainID: domain, tenant: tenant)
    let encoded = try await route.forward(
      operation: .catalogHead,
      tenant: tenant.identifier.rawValue,
      payload: payload
    )
    let forwarded = try JSONDecoder().decode(CatalogBrokerForwardRequest.self, from: encoded)
    #expect(forwarded.operation == .catalogHead)
    #expect(forwarded.payload == payload)
    #expect(forwarded.context.domainID == domain)
    #expect(forwarded.context.tenantID == tenant.identifier)
    #expect(forwarded.context.generation == tenant.generation)
    let resolveEncoded = try await route.forward(
      operation: .criticalReadinessResolve,
      tenant: tenant.identifier.rawValue,
      payload: payload
    )
    let resolved = try JSONDecoder().decode(
      CatalogBrokerForwardRequest.self,
      from: resolveEncoded
    )
    #expect(resolved.operation == .criticalReadinessResolve)
    #expect(resolved.context.domainID == domain)
    let ackEncoded = try await route.forward(
      operation: .criticalReadinessFetchAck,
      tenant: tenant.identifier.rawValue,
      payload: payload
    )
    let ackForwarded = try JSONDecoder().decode(
      CatalogBrokerForwardRequest.self,
      from: ackEncoded
    )
    #expect(ackForwarded.operation == .criticalReadinessFetchAck)
    #expect(ackForwarded.context.domainID == domain)
    await #expect(throws: CatalogTransportError.operationNotForwardable) {
      try await route.forward(
        operation: .tenantPrepare,
        tenant: tenant.identifier.rawValue,
        payload: payload
      )
    }

    let other = try CatalogTenant(identifier: CatalogTenantID("tenant-2"), generation: 4)
    await #expect(throws: CatalogTransportError.bindingConflict) {
      try await route.bind(domainID: domain, tenant: other)
    }
  }
}

private let materializedRoot = try! CatalogObjectID("00000000000000000000000000000001")
private let materializedObjectOne = try! CatalogObjectID("10000000000000000000000000000001")
private let materializedObjectTwo = try! CatalogObjectID("10000000000000000000000000000002")

private func materializationBinding() throws -> CatalogFileProviderBinding {
  try CatalogFileProviderBinding(
    domainID: testDomainID(),
    tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 4),
    rootID: materializedRoot,
    accessMode: .readWrite
  )
}

private func eventually(_ predicate: @escaping @Sendable () async -> Bool) async -> Bool {
  for _ in 0 ..< 1_000 {
    if await predicate() { return true }
    await Task.yield()
  }
  return false
}

private struct FixedMaterializedSetSource: CatalogMaterializedSetSource {
  let identity: Data?
  let identifiers: [NSFileProviderItemIdentifier]

  func backingStoreIdentity() async -> Data? { identity }
  func containerIdentifiers() async throws -> [NSFileProviderItemIdentifier] { identifiers }
}

private struct FailingMaterializedSetSource: CatalogMaterializedSetSource {
  let identity: Data

  func backingStoreIdentity() async -> Data? { identity }
  func containerIdentifiers() async throws -> [NSFileProviderItemIdentifier] {
    throw CocoaError(.fileReadUnknown)
  }
}

private actor BlockingMaterializedSetSource: CatalogMaterializedSetSource {
  let identity: Data
  private var calls = 0
  private var blocked = false
  private var continuation: CheckedContinuation<Void, Never>?

  init(identity: Data) {
    self.identity = identity
  }

  func backingStoreIdentity() async -> Data? { identity }

  func containerIdentifiers() async throws -> [NSFileProviderItemIdentifier] {
    calls += 1
    if calls == 1 {
      blocked = true
      await withCheckedContinuation { continuation = $0 }
      return [NSFileProviderItemIdentifier(materializedObjectOne.rawValue)]
    }
    return [NSFileProviderItemIdentifier(materializedObjectTwo.rawValue)]
  }

  func isBlocked() -> Bool { blocked }

  func release() {
    continuation?.resume()
    continuation = nil
  }
}

private actor MaterializationTransport: CatalogTransport {
  private var begins = 0
  private var suspends = 0
  private var stages: [[String]] = []
  private var commits = 0

  func bind(domainID _: CatalogDomainID, tenant _: CatalogTenant) async throws {}

  nonisolated func activationNotifications() -> CatalogNotificationFeed { .empty }

  func unary(operation: CatalogOperation, tenant _: String, payload: Data) async throws -> Data {
    let decoder = JSONDecoder()
    let encoder = JSONEncoder()
    switch operation {
    case .materializationSnapshotBegin:
      _ = try decoder.decode(CatalogBeginMaterializationSnapshotRequest.self, from: payload)
      begins += 1
      return try encoder.encode(
        CatalogBeginMaterializationSnapshotResponse(code: .ok, message: "", epoch: UInt64(begins))
      )
    case .materializationSnapshotSuspend:
      _ = try decoder.decode(CatalogSuspendMaterializationSnapshotRequest.self, from: payload)
      suspends += 1
      return try encoder.encode(CatalogSuspendMaterializationSnapshotResponse(code: .ok, message: ""))
    case .materializationSnapshotStagePage:
      let request = try decoder.decode(
        CatalogStageMaterializationSnapshotPageRequest.self,
        from: payload
      )
      stages.append(request.containerIDs.map(\.rawValue))
      return try encoder.encode(CatalogStageMaterializationSnapshotPageResponse(code: .ok, message: ""))
    case .materializationSnapshotCommit:
      _ = try decoder.decode(CatalogCommitMaterializationSnapshotRequest.self, from: payload)
      commits += 1
      return try encoder.encode(
        CatalogCommitMaterializationSnapshotResponse(
          code: .ok,
          message: "",
          revision: UInt64(commits),
          added: 0,
          removed: 0
        )
      )
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
    operation _: CatalogOperation,
    tenant _: String,
    payload _: Data,
    body _: CatalogUpload
  ) async throws -> Data {
    throw CatalogTransportError.remote("unexpected upload")
  }

  func beginCount() -> Int { begins }
  func suspendCount() -> Int { suspends }
  func stageCount() -> Int { stages.count }
  func stagedIDs() -> [String] { stages.flatMap { $0 } }
  func commitCount() -> Int { commits }
}
