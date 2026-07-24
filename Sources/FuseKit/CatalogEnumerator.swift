@preconcurrency import FileProvider
import Foundation

/// CatalogEnumerator pages one pinned snapshot and consumes only catalog deltas.
public final class CatalogEnumerator: NSObject, NSFileProviderEnumerator, @unchecked Sendable {
  public enum Scope: Sendable {
    case container(CatalogObjectID)
    case workingSet
  }

  private let client: CatalogClient
  private let binding: CatalogFileProviderBinding
  private let scope: Scope
  private let activation: CatalogActivationInbox
  private let bindingGate: CatalogBindingGate
  private let lock = NSLock()
  private var tasks: [UUID: Task<Void, Never>] = [:]

  init(
    client: CatalogClient,
    binding: CatalogFileProviderBinding,
    scope: Scope,
    activation: CatalogActivationInbox,
    bindingGate: CatalogBindingGate
  ) {
    self.client = client
    self.binding = binding
    self.scope = scope
    self.activation = activation
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
        let response = try await self.client.snapshot(
          tenant: self.binding.tenant,
          revision: token.revision,
          scope: self.catalogScope(),
          after: token.after,
          limit: self.binding.pageSize
        )
        try Task.checkCancellation()
        let items = response.objects.map {
          CatalogFileProviderItem(
            object: $0,
            rootID: self.binding.rootID,
            accessMode: self.binding.accessMode
          )
        }
        observer.didEnumerate(items)
        let next = try response.next.map {
          try NSFileProviderPage(
            JSONEncoder().encode(
              CatalogEnumerationPageToken(
                version: 1,
                context: self.tokenContext(),
                revision: token.revision,
                after: $0
              )
            )
          )
        }
        observer.finishEnumerating(upTo: next)
      } catch is CancellationError {
        return
      } catch {
        observer.finishEnumeratingWithError(CatalogEnumerationError.page(error))
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
        try await self.activation.checkHealthy()
        let cursor = try self.decodeAnchor(anchor)
        let response = try await self.client.changes(
          tenant: self.binding.tenant,
          since: cursor,
          scope: self.catalogScope(),
          limit: self.binding.pageSize
        )
        try Task.checkCancellation()
        CatalogChangeEmitter(binding: self.binding).emit(response.changes, to: observer)
        observer.finishEnumeratingChanges(
          upTo: self.anchor(response.next),
          moreComing: !response.complete
        )
        if response.complete {
          do {
            try await self.activation.acknowledgeObserved(
              target: self.signalTarget(),
              upTo: response.head
            )
          } catch {
            await self.activation.fail(error)
          }
        }
      } catch is CancellationError {
        return
      } catch {
        observer.finishEnumeratingWithError(CatalogEnumerationError.change(error))
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
          self.anchor(
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

  private func pageToken(_ page: NSFileProviderPage) async throws -> CatalogEnumerationPageToken {
    if page.rawValue == NSFileProviderPage.initialPageSortedByName as Data
      || page.rawValue == NSFileProviderPage.initialPageSortedByDate as Data {
      return try await CatalogEnumerationPageToken(
        version: 1,
        context: tokenContext(),
        revision: client.head(tenant: binding.tenant),
        after: nil
      )
    }
    guard
      let token = try? JSONDecoder().decode(
        CatalogEnumerationPageToken.self,
        from: page.rawValue
      ),
      token.version == 1,
      token.context == tokenContext(),
      token.revision != 0
    else {
      throw NSFileProviderError(.pageExpired)
    }
    return token
  }

  private func tokenContext() -> CatalogEnumerationTokenContext {
    switch scope {
    case let .container(parentID):
      CatalogEnumerationTokenContext(
        domainID: binding.domainID,
        tenantID: binding.tenant.identifier,
        generation: binding.tenant.generation,
        rootID: binding.rootID,
        scope: .container,
        parentID: parentID
      )
    case .workingSet:
      CatalogEnumerationTokenContext(
        domainID: binding.domainID,
        tenantID: binding.tenant.identifier,
        generation: binding.tenant.generation,
        rootID: binding.rootID,
        scope: .workingSet,
        parentID: nil
      )
    }
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

  func anchor(_ cursor: CatalogChangeCursor) -> NSFileProviderSyncAnchor {
    do {
      return try NSFileProviderSyncAnchor(
        JSONEncoder().encode(
          CatalogEnumerationChangeAnchor(version: 1, context: tokenContext(), cursor: cursor)
        )
      )
    } catch {
      preconditionFailure("FuseKit change anchor encoding failed: \(error)")
    }
  }

  func decodeAnchor(_ anchor: NSFileProviderSyncAnchor) throws -> CatalogChangeCursor {
    guard let value = try? JSONDecoder().decode(
      CatalogEnumerationChangeAnchor.self,
      from: anchor.rawValue
    ),
      value.version == 1,
      value.context == tokenContext()
    else {
      throw NSFileProviderError(.syncAnchorExpired)
    }
    return value.cursor
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
