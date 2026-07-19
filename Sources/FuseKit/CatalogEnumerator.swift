@preconcurrency import FileProvider
import Foundation

/// CatalogEnumerator pages one pinned snapshot and consumes only catalog deltas.
public final class CatalogEnumerator: NSObject, NSFileProviderEnumerator, @unchecked Sendable {
  public enum Scope: Sendable {
    case container(CatalogObjectID)
    case workingSet
  }

  private struct PageToken: Codable {
    let revision: UInt64
    let after: String?
  }

  private struct ChangeAnchor: Codable {
    let version: UInt8
    let cursor: CatalogChangeCursor
  }

  private let client: CatalogClient
  private let binding: CatalogFileProviderBinding
  private let scope: Scope
  private let convergence: CatalogConvergenceInbox
  private let bindingGate: CatalogBindingGate
  private let lock = NSLock()
  private var tasks: [UUID: Task<Void, Never>] = [:]

  init(
    client: CatalogClient,
    binding: CatalogFileProviderBinding,
    scope: Scope,
    convergence: CatalogConvergenceInbox,
    bindingGate: CatalogBindingGate
  ) {
    self.client = client
    self.binding = binding
    self.scope = scope
    self.convergence = convergence
    self.bindingGate = bindingGate
  }

  public func invalidate() {
    lock.lock()
    let active = Array(tasks.values)
    tasks.removeAll()
    lock.unlock()
    for task in active {
      task.cancel()
    }
  }

  public func enumerateItems(
    for observer: any NSFileProviderEnumerationObserver,
    startingAt page: NSFileProviderPage
  ) {
    run {
      do {
        try await self.bindingGate.bind()
        let token = try await self.pageToken(page)
        let after = try token.after.map(CatalogObjectID.init)
        let response = try await self.client.snapshot(
          tenant: self.binding.tenant,
          revision: token.revision,
          scope: self.catalogScope(),
          after: after,
          limit: self.binding.pageSize
        )
        try Task.checkCancellation()
        let items = response.objects.map {
          CatalogFileProviderItem(object: $0, rootID: self.binding.rootID)
        }
        observer.didEnumerate(items)
        let next = try response.next.map {
          try NSFileProviderPage(
            JSONEncoder().encode(
              PageToken(
                revision: token.revision,
                after: $0.rawValue
              )
            )
          )
        }
        observer.finishEnumerating(upTo: next)
      } catch is CancellationError {
        return
      } catch {
        observer.finishEnumeratingWithError(Self.fileProviderError(error))
      }
    }
  }

  public func enumerateChanges(
    for observer: any NSFileProviderChangeObserver,
    from anchor: NSFileProviderSyncAnchor
  ) {
    run {
      do {
        try await self.bindingGate.bind()
        try await self.convergence.checkHealthy()
        let cursor = try Self.decodeAnchor(anchor)
        let response = try await self.client.changes(
          tenant: self.binding.tenant,
          since: cursor,
          scope: self.catalogScope(),
          limit: self.binding.pageSize
        )
        try Task.checkCancellation()
        self.emit(response.changes, to: observer)
        observer.finishEnumeratingChanges(
          upTo: Self.anchor(response.next),
          moreComing: !response.complete
        )
        if response.complete {
          do {
            try await self.convergence.acknowledgeObserved(
              target: self.signalTarget(),
              upTo: response.head
            )
          } catch {
            await self.convergence.fail(error)
          }
        }
      } catch is CancellationError {
        return
      } catch {
        observer.finishEnumeratingWithError(Self.fileProviderError(error))
      }
    }
  }

  public func currentSyncAnchor(
    completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void
  ) {
    let completion = Completion(completionHandler)
    run {
      do {
        try await self.bindingGate.bind()
        let head = try await self.client.head(tenant: self.binding.tenant)
        try Task.checkCancellation()
        completion.call(
          Self.anchor(
            CatalogChangeCursor(
              revision: head,
              sequence: CatalogProtocol.changeCursorCompleteSequence
            )
          )
        )
      } catch is CancellationError {
        return
      } catch {
        completion.call(nil)
      }
    }
  }

  private func pageToken(_ page: NSFileProviderPage) async throws -> PageToken {
    if page.rawValue == NSFileProviderPage.initialPageSortedByName as Data
      || page.rawValue == NSFileProviderPage.initialPageSortedByDate as Data {
      return try await PageToken(revision: client.head(tenant: binding.tenant), after: nil)
    }
    return try JSONDecoder().decode(PageToken.self, from: page.rawValue)
  }

  private func catalogScope() throws -> CatalogEnumerationScope {
    switch scope {
    case let .container(parentID):
      try CatalogEnumerationScope(kind: .container, parentID: parentID)
    case .workingSet:
      try CatalogEnumerationScope(kind: .workingSet)
    }
  }

  private func signalTarget() throws -> CatalogSignalTarget {
    switch scope {
    case let .container(parentID):
      try CatalogSignalTarget(kind: .container, parentID: parentID)
    case .workingSet:
      try CatalogSignalTarget(kind: .workingSet)
    }
  }

  private func identifier(_ objectID: CatalogObjectID) -> NSFileProviderItemIdentifier {
    objectID == binding.rootID ? .rootContainer : NSFileProviderItemIdentifier(objectID.rawValue)
  }

  private func emit(
    _ changes: [CatalogChange],
    to observer: any NSFileProviderChangeObserver
  ) {
    let deletions = changes.filter { $0.kind == .delete }.map { identifier($0.object.id) }
    let updates = changes.filter { $0.kind == .upsert }.map {
      CatalogFileProviderItem(object: $0.object, rootID: binding.rootID)
    }
    if !deletions.isEmpty {
      observer.didDeleteItems(withIdentifiers: deletions)
    }
    if !updates.isEmpty {
      observer.didUpdate(updates)
    }
  }

  private func run(_ operation: @escaping @Sendable () async -> Void) {
    let id = UUID()
    lock.lock()
    let task = Task { [weak self] in
      await operation()
      self?.removeTask(id)
    }
    tasks[id] = task
    lock.unlock()
  }

  func activeTaskCount() -> Int {
    lock.withLock { tasks.count }
  }

  private func removeTask(_ id: UUID) {
    lock.lock()
    tasks.removeValue(forKey: id)
    lock.unlock()
  }

  static func anchor(_ cursor: CatalogChangeCursor) -> NSFileProviderSyncAnchor {
    do {
      return try NSFileProviderSyncAnchor(
        JSONEncoder().encode(ChangeAnchor(version: 1, cursor: cursor))
      )
    } catch {
      preconditionFailure("FuseKit change anchor encoding failed: \(error)")
    }
  }

  static func decodeAnchor(_ anchor: NSFileProviderSyncAnchor) throws -> CatalogChangeCursor {
    guard let value = try? JSONDecoder().decode(ChangeAnchor.self, from: anchor.rawValue),
          value.version == 1
    else {
      throw NSFileProviderError(.syncAnchorExpired)
    }
    return value.cursor
  }

  private static func fileProviderError(_ error: Error) -> Error {
    if case let CatalogClientError.response(code, _) = error, code == .staleAnchor {
      return NSFileProviderError(.syncAnchorExpired)
    }
    return error
  }
}

private final class Completion<Value>: @unchecked Sendable {
  private let body: (Value) -> Void

  init(_ body: @escaping (Value) -> Void) {
    self.body = body
  }

  func call(_ value: Value) {
    body(value)
  }
}
