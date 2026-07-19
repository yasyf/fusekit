import Foundation
@testable import FuseKit
import Testing

@Suite("Broker session admission")
struct BrokerSessionTests {
  @Test
  func liveSessionsAreNeverEvictedAtCapacity() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 2)
    let first = TestEventSession()
    let second = TestEventSession()
    let rejected = TestEventSession()

    try await sessions.bind(first, to: binding(account: "account-1"))
    try await sessions.bind(second, to: binding(account: "account-2"))
    await #expect(throws: CatalogSessionError.capacity) {
      try await sessions.bind(rejected, to: binding(account: "account-3"))
    }

    #expect(await sessions.sessionCount() == 2)
    _ = try await sessions.authorize(first, tenant: "tenant-1")
    _ = try await sessions.authorize(second, tenant: "tenant-1")
  }

  @Test
  func identicalSecondBindIsRejected() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 1)
    let session = TestEventSession()
    let accepted = try binding(account: "account-1")
    try await sessions.bind(session, to: accepted)

    await #expect(throws: CatalogSessionError.rebind) {
      try await sessions.bind(session, to: accepted)
    }
    #expect(await sessions.sessionCount() == 1)
  }

  @Test
  func disconnectedSlotIsReclaimedForNextBind() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 1)
    let disconnected = TestEventSession()
    let replacement = TestEventSession()
    try await sessions.bind(disconnected, to: binding(account: "account-1"))

    disconnected.disconnect()
    try await sessions.bind(replacement, to: binding(account: "account-2"))

    #expect(await sessions.sessionCount() == 1)
    _ = try await sessions.authorize(replacement, tenant: "tenant-1")
    await #expect(throws: CatalogSessionError.unbound) {
      _ = try await sessions.authorize(disconnected, tenant: "tenant-1")
    }
  }

  @Test
  func notificationRoutesOnlyToExactBoundGeneration() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 2)
    let oldGeneration = TestEventSession()
    let newGeneration = TestEventSession()
    let oldBinding = try binding(account: "account-1", generation: 1)
    let newBinding = try binding(account: "account-1", generation: 2)
    try await sessions.bind(oldGeneration, to: oldBinding)
    try await sessions.bind(newGeneration, to: newBinding)

    try await sessions.publish(notification(binding: newBinding, revision: 1))

    #expect(oldGeneration.eventCount() == 0)
    #expect(newGeneration.eventCount() == 1)
  }

  @Test
  func forwardEnvelopeUsesOnlyAcceptedServerBinding() throws {
    let binding = try binding(account: "account-1", generation: 9)
    let payload = Data("request".utf8)
    let envelope = binding.forwarding(operation: .catalogLookup, payload: payload)

    #expect(envelope.context.domainID == binding.domainID)
    #expect(envelope.context.tenantID == binding.tenantID)
    #expect(envelope.context.generation == binding.generation)
    #expect(envelope.operation == .catalogLookup)
    #expect(envelope.payload == payload)
  }

  @Test
  func removingDomainRevokesLiveSessionAndClearsRoute() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 2)
    let removed = TestEventSession()
    let oldBinding = try binding(account: "account-1", generation: 1)
    try await sessions.bind(removed, to: oldBinding)
    try await sessions.publish(notification(binding: oldBinding, revision: 1))

    await sessions.retire(oldBinding.domainID)

    #expect(await sessions.routeCount() == 0)
    #expect(await sessions.sessionCount() == 0)
    #expect(await sessions.retainedSessionCount() == 1)
    await #expect(throws: CatalogSessionError.revoked) {
      _ = try await sessions.authorize(removed, tenant: oldBinding.tenantID.rawValue)
    }
    await #expect(throws: CatalogSessionError.revoked) {
      try await sessions.bind(removed, to: oldBinding)
    }

    removed.disconnect()
    let replacement = TestEventSession()
    let newBinding = try binding(account: "account-1", generation: 2)
    try await sessions.bind(replacement, to: newBinding)
    _ = try await sessions.authorize(replacement, tenant: newBinding.tenantID.rawValue)
    #expect(await sessions.retainedSessionCount() == 1)
  }

  @Test
  func generationChurnDoesNotAccumulateRoutesOrClosedSessions() async throws {
    let sessions = CatalogExtensionSessions(maximumSessions: 1)
    for generation in UInt64(1) ... 20 {
      let session = TestEventSession()
      let current = try binding(account: "account-1", generation: generation)
      try await sessions.bind(session, to: current)
      try await sessions.publish(notification(binding: current, revision: generation))
      await sessions.retire(current.domainID)
      #expect(await sessions.routeCount() == 0)
      #expect(await sessions.retainedSessionCount() == 1)
      session.disconnect()
    }
    #expect(await sessions.retainedSessionCount() == 0)
  }

  private func binding(account: String, generation: UInt64 = 1) throws -> CatalogSessionBinding {
    let accountID = try CatalogAccountInstanceID(account)
    return try CatalogSessionBinding(
      CatalogBrokerBindDomainRequest(
        domainID: CatalogDomainID.derived(
          ownerID: CatalogOwnerID("owner-1"),
          accountInstanceID: accountID
        ),
        tenantID: CatalogTenantID("tenant-1"),
        generation: generation
      )
    )
  }

  private func notification(
    binding: CatalogSessionBinding,
    revision: UInt64
  ) throws -> CatalogConvergenceNotification {
    try CatalogConvergenceNotification(
      tenantID: binding.tenantID,
      domainID: binding.domainID,
      generation: binding.generation,
      revision: revision,
      catalogRevision: revision,
      sourceRevision: revision,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogMutationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      affectedKeys: ["settings.json"],
      targets: [CatalogSignalTarget(kind: .workingSet)]
    )
  }
}

private final class TestEventSession: CatalogEventSession, @unchecked Sendable {
  private let lock = NSLock()
  private var connected = true
  private var waiters: [CheckedContinuation<Void, Never>] = []
  private var events: [Data] = []

  var isConnected: Bool {
    lock.withLock { connected }
  }

  func waitUntilClosed() async {
    await withCheckedContinuation { continuation in
      lock.lock()
      guard connected else {
        lock.unlock()
        continuation.resume()
        return
      }
      waiters.append(continuation)
      lock.unlock()
    }
  }

  func pushEvent(topic _: String, payload: Data) async throws {
    guard isConnected else { throw CatalogSessionError.disconnected }
    lock.withLock { events.append(payload) }
  }

  func eventCount() -> Int {
    lock.withLock { events.count }
  }

  func disconnect() {
    lock.lock()
    guard connected else {
      lock.unlock()
      return
    }
    connected = false
    let pending = waiters
    waiters.removeAll()
    lock.unlock()
    for waiter in pending {
      waiter.resume()
    }
  }
}
