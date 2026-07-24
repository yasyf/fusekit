@preconcurrency import FileProvider
import Foundation

enum CatalogMaterializedSetError: Error, Equatable, Sendable {
  case managerUnavailable
  case repeatedPage
  case invalidContainerIdentifier
  case tooManyPages
}

protocol CatalogMaterializedSetSource: Sendable {
  func backingStoreIdentity() async -> Data?
  func containerIdentifiers() async throws -> [NSFileProviderItemIdentifier]
}

final class NativeCatalogMaterializedSetSource: CatalogMaterializedSetSource,
  @unchecked Sendable {
  private let domain: NSFileProviderDomain
  private let manager: NSFileProviderManager

  init(domain: NSFileProviderDomain) throws {
    guard let manager = NSFileProviderManager(for: domain) else {
      throw CatalogMaterializedSetError.managerUnavailable
    }
    self.domain = domain
    self.manager = manager
  }

  func backingStoreIdentity() async -> Data? {
    domain.backingStoreIdentity
  }

  func containerIdentifiers() async throws -> [NSFileProviderItemIdentifier] {
    let enumerator = manager.enumeratorForMaterializedItems()
    defer { enumerator.invalidate() }
    var identifiers: [NSFileProviderItemIdentifier] = []
    var page = NSFileProviderPage(rawValue: Data())
    var seen: Set<Data> = []
    while true {
      try Task.checkCancellation()
      guard seen.insert(page.rawValue).inserted else {
        throw CatalogMaterializedSetError.repeatedPage
      }
      let result = try await Self.enumerate(enumerator, page: page)
      identifiers.append(contentsOf: result.identifiers)
      guard let next = result.next else { return identifiers }
      page = next
    }
  }

  private static func enumerate(
    _ enumerator: any NSFileProviderEnumerator,
    page: NSFileProviderPage
  ) async throws -> MaterializedPage {
    let observer = MaterializedPageObserver()
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        observer.start(continuation)
        enumerator.enumerateItems(for: observer, startingAt: page)
      }
    } onCancel: {
      observer.cancel()
      enumerator.invalidate()
    }
  }
}

private struct MaterializedPage: Sendable {
  let identifiers: [NSFileProviderItemIdentifier]
  let next: NSFileProviderPage?
}

private final class MaterializedPageObserver: NSObject, NSFileProviderEnumerationObserver,
  @unchecked Sendable {
  private let lock = NSLock()
  private var continuation: CheckedContinuation<MaterializedPage, any Error>?
  private var identifiers: [NSFileProviderItemIdentifier] = []
  private var outcome: Result<MaterializedPage, any Error>?

  func start(_ continuation: CheckedContinuation<MaterializedPage, any Error>) {
    let completed = lock.withLock { () -> Result<MaterializedPage, any Error>? in
      guard self.continuation == nil else { return .failure(CancellationError()) }
      if let outcome { return outcome }
      self.continuation = continuation
      return nil
    }
    if let completed { continuation.resume(with: completed) }
  }

  func didEnumerate(_ updatedItems: [any NSFileProviderItem]) {
    lock.withLock {
      guard outcome == nil else { return }
      identifiers.append(contentsOf: updatedItems.map(\.itemIdentifier))
    }
  }

  func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
    let settled = lock.withLock {
      () -> (CheckedContinuation<MaterializedPage, any Error>?, MaterializedPage)? in
      guard outcome == nil else { return nil }
      let page = MaterializedPage(identifiers: identifiers, next: nextPage)
      outcome = .success(page)
      let value = continuation
      continuation = nil
      return (value, page)
    }
    if let settled { settled.0?.resume(returning: settled.1) }
  }

  func finishEnumeratingWithError(_ error: any Error) {
    finish(.failure(error))
  }

  func cancel() {
    finish(.failure(CancellationError()))
  }

  private func finish(_ result: Result<MaterializedPage, any Error>) {
    let continuation = lock.withLock { () -> CheckedContinuation<MaterializedPage, any Error>? in
      guard outcome == nil else { return nil }
      outcome = result
      let value = self.continuation
      self.continuation = nil
      return value
    }
    continuation?.resume(with: result)
  }
}

final class CatalogMaterializationCoordinator: @unchecked Sendable {
  private let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private let bindingGate: CatalogBindingGate
  private let source: any CatalogMaterializedSetSource
  private let lock = NSLock()
  private var dirty = false
  private var invalidated = false
  private var task: Task<Void, Never>?

  init(
    binding: CatalogFileProviderBinding,
    client: CatalogClient,
    bindingGate: CatalogBindingGate,
    source: any CatalogMaterializedSetSource
  ) {
    self.binding = binding
    self.client = client
    self.bindingGate = bindingGate
    self.source = source
  }

  deinit {
    invalidate()
  }

  func markDirty() {
    lock.withLock {
      guard !invalidated else { return }
      dirty = true
      guard task == nil else { return }
      task = Task { [weak self] in
        await self?.drain()
      }
    }
  }

  func invalidate() {
    let current = lock.withLock { () -> Task<Void, Never>? in
      guard !invalidated else { return nil }
      invalidated = true
      dirty = false
      return task
    }
    current?.cancel()
  }

  private func drain() async {
    while takeDirty() {
      do {
        try await reconcile()
      } catch is CancellationError {
        return
      } catch {
        continue
      }
    }
  }

  private func takeDirty() -> Bool {
    lock.withLock {
      guard !invalidated else {
        task = nil
        return false
      }
      guard dirty else {
        task = nil
        return false
      }
      dirty = false
      return true
    }
  }

  private func superseded() -> Bool {
    lock.withLock { invalidated || dirty }
  }

  private func scheduleReplacement() {
    lock.withLock {
      guard !invalidated else { return }
      dirty = true
    }
  }

  private func reconcile() async throws {
    try Task.checkCancellation()
    try await bindingGate.bind()
    guard let backingStoreIdentity = await source.backingStoreIdentity() else {
      try await client.suspendMaterialization(binding: binding)
      return
    }
    let snapshotID = try CatalogMaterializationSnapshotID(
      UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
    )
    _ = try await client.beginMaterializationSnapshot(
      binding: binding,
      snapshotID: snapshotID,
      backingStoreIdentity: backingStoreIdentity
    )
    let identifiers = try await source.containerIdentifiers()
    if superseded() { return }
    guard await source.backingStoreIdentity() == backingStoreIdentity else {
      scheduleReplacement()
      return
    }
    let containerIDs = try canonicalContainerIDs(identifiers)
    let pageSize = Int(CatalogProtocol.maxPageSize)
    let count = max(1, (containerIDs.count + pageSize - 1) / pageSize)
    guard count <= Int(UInt32.max) else { throw CatalogMaterializedSetError.tooManyPages }
    for index in 0 ..< count {
      let lower = index * pageSize
      let upper = min(containerIDs.count, lower + pageSize)
      let page = lower < upper ? Array(containerIDs[lower ..< upper]) : []
      try await client.stageMaterializationPage(
        binding: binding,
        snapshotID: snapshotID,
        backingStoreIdentity: backingStoreIdentity,
        sequence: UInt32(index),
        containerIDs: page
      )
      if superseded() { return }
    }
    guard await source.backingStoreIdentity() == backingStoreIdentity else {
      scheduleReplacement()
      return
    }
    _ = try await client.commitMaterializationSnapshot(
      binding: binding,
      snapshotID: snapshotID,
      backingStoreIdentity: backingStoreIdentity,
      pageCount: UInt32(count)
    )
  }

  private func canonicalContainerIDs(
    _ identifiers: [NSFileProviderItemIdentifier]
  ) throws -> [CatalogObjectID] {
    var values: Set<CatalogObjectID> = []
    for identifier in identifiers {
      if identifier == .rootContainer {
        values.insert(binding.rootID)
        continue
      }
      guard identifier != .workingSet,
            let objectID = try? CatalogObjectID(identifier.rawValue)
      else { throw CatalogMaterializedSetError.invalidContainerIdentifier }
      values.insert(objectID)
    }
    return values.sorted { $0.rawValue < $1.rawValue }
  }
}
