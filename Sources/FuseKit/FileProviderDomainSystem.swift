@preconcurrency import FileProvider

struct FileProviderDomainHandle: @unchecked Sendable {
  let domain: NSFileProviderDomain

  var identifier: String {
    domain.identifier.rawValue
  }
}

protocol FileProviderDomainBackend: Sendable {
  func domains() async throws -> [FileProviderDomainHandle]
  func add(_ domain: FileProviderDomainHandle) async throws
  func remove(_ domain: FileProviderDomainHandle) async throws
  func publicPath(for domain: FileProviderDomainHandle) async throws -> String
  func signal(
    domain: FileProviderDomainHandle,
    targets: [CatalogSignalTarget]
  ) async throws
}

final class NativeFileProviderDomainBackend: FileProviderDomainBackend, @unchecked Sendable {
  func domains() async throws -> [FileProviderDomainHandle] {
    try await NSFileProviderManager.domains().map(FileProviderDomainHandle.init)
  }

  func add(_ domain: FileProviderDomainHandle) async throws {
    try await NSFileProviderManager.add(domain.domain)
  }

  func remove(_ domain: FileProviderDomainHandle) async throws {
    try await NSFileProviderManager.remove(domain.domain)
  }

  func publicPath(for domain: FileProviderDomainHandle) async throws -> String {
    guard let manager = NSFileProviderManager(for: domain.domain) else {
      throw FileProviderDomainSystem.SystemError.registrationMetadataMissing
    }
    return try await manager.getUserVisibleURL(for: .rootContainer).path
  }

  func signal(
    domain: FileProviderDomainHandle,
    targets: [CatalogSignalTarget]
  ) async throws {
    guard let manager = NSFileProviderManager(for: domain.domain) else {
      throw FileProviderDomainSystem.SystemError.domainNotFound
    }
    for target in targets {
      let identifier: NSFileProviderItemIdentifier
      switch target.kind {
      case .workingSet:
        identifier = .workingSet
      case .container:
        guard let parentID = target.parentID else {
          throw FileProviderDomainSystem.SystemError.invalidTarget
        }
        identifier = NSFileProviderItemIdentifier(parentID.rawValue)
      }
      try await manager.signalEnumerator(for: identifier)
    }
  }
}

actor FileProviderDomainSystem: CatalogDomainSystem {
  enum SystemError: Error {
    case conflictingRegistration
    case domainNotFound
    case invalidTarget
    case registrationMetadataMissing
    case registrationMismatch
  }

  private struct IndexedDomain {
    let handle: FileProviderDomainHandle
    let metadata: CatalogDomainMetadata
  }

  private struct IndexedObservation {
    let handle: FileProviderDomainHandle
    let managed: IndexedDomain?
  }

  private let backend: any FileProviderDomainBackend
  private var indexed = false
  private var domainsByID: [CatalogDomainID: IndexedDomain] = [:]
  private var observationsByID: [CatalogObservedDomainID: IndexedObservation] = [:]
  private var orderedObservedIDs: [CatalogObservedDomainID] = []
  private var indexRevision: UInt64 = 0
  private var failureRepairRevision: UInt64?

  init(backend: any FileProviderDomainBackend = NativeFileProviderDomainBackend()) {
    self.backend = backend
  }

  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain {
    guard registration.generation > 0 else { throw SystemError.conflictingRegistration }
    try await ensureIndex()
    let observedID = try CatalogObservedDomainID(observing: registration.domainID.rawValue)
    if let observation = observationsByID[observedID] {
      guard let existing = observation.managed else {
        throw SystemError.conflictingRegistration
      }
      try validate(existing, registration: registration)
      do {
        return try await registered(existing)
      } catch {
        if try await repairAfterFailure() {
          if let repaired = domainsByID[registration.domainID] {
            try validate(repaired, registration: registration)
            return try await registered(repaired)
          }
        } else {
          throw error
        }
      }
    }

    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    domain.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    let handle = FileProviderDomainHandle(domain: domain)
    do {
      try await backend.add(handle)
    } catch {
      if try await repairAfterFailure(), let existing = domainsByID[registration.domainID] {
        try validate(existing, registration: registration)
        return try await registered(existing)
      }
      throw error
    }
    let entry = IndexedDomain(
      handle: handle, metadata: CatalogDomainMetadata(registration: registration)
    )
    insert(entry, observedID: observedID)
    return try await registered(entry)
  }

  func remove(_ observedID: CatalogObservedDomainID) async throws -> Bool {
    try await ensureIndex()
    guard let existing = observationsByID[observedID] else { return true }
    do {
      try await backend.remove(existing.handle)
    } catch {
      if try await repairAfterFailure(), observationsByID[observedID] == nil {
        return true
      }
      throw error
    }
    removeFromIndex(observedID)
    return true
  }

  func list(
    after: CatalogObservedDomainID?, limit: Int
  ) async throws -> [CatalogObservedDomain] {
    guard limit > 0, limit < Int.max else { throw SystemError.invalidTarget }
    try await ensureIndex()
    do {
      return try await listIndexed(after: after, limit: limit)
    } catch {
      if try await repairAfterFailure() {
        return try await listIndexed(after: after, limit: limit)
      }
      throw error
    }
  }

  private func listIndexed(
    after: CatalogObservedDomainID?,
    limit: Int
  ) async throws -> [CatalogObservedDomain] {
    let start = firstIndex(after: after)
    let end = min(orderedObservedIDs.count, start + limit + 1)
    var result: [CatalogObservedDomain] = []
    result.reserveCapacity(end - start)
    for observedID in orderedObservedIDs[start ..< end] {
      guard let indexed = observationsByID[observedID] else {
        throw SystemError.registrationMismatch
      }
      let managed: CatalogRegisteredDomain? = if let indexed = indexed.managed {
        try await registered(indexed)
      } else {
        nil
      }
      result.append(CatalogObservedDomain(observedID: observedID, managed: managed))
    }
    return result
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    try await ensureIndex()
    guard let indexed = domainsByID[binding.domainID] else {
      throw SystemError.domainNotFound
    }
    guard indexed.metadata.domainID == binding.domainID,
          indexed.metadata.tenantID == binding.tenantID,
          indexed.metadata.generation == binding.generation
    else {
      throw SystemError.registrationMismatch
    }
  }

  func signal(domainID: CatalogDomainID, targets: [CatalogSignalTarget]) async throws {
    try await ensureIndex()
    guard let indexed = domainsByID[domainID] else {
      throw SystemError.domainNotFound
    }
    do {
      try await backend.signal(domain: indexed.handle, targets: targets)
    } catch {
      if try await repairAfterFailure(), let repaired = domainsByID[domainID],
         repaired.metadata == indexed.metadata {
        try await backend.signal(domain: repaired.handle, targets: targets)
        return
      }
      throw error
    }
  }

  private func ensureIndex() async throws {
    guard !indexed else { return }
    try await rebuildIndex()
  }

  private func rebuildIndex() async throws {
    var rebuilt: [CatalogDomainID: IndexedDomain] = [:]
    var observed: [CatalogObservedDomainID: IndexedObservation] = [:]
    for handle in try await backend.domains() {
      let observedID = try CatalogObservedDomainID(observing: handle.identifier)
      guard observed[observedID] == nil else {
        throw SystemError.conflictingRegistration
      }
      var managed: IndexedDomain?
      if CatalogDomainMetadata.declaresMetadata(handle.domain),
         let domainMetadata = try? CatalogDomainMetadata(domain: handle.domain) {
        guard rebuilt[domainMetadata.domainID] == nil else {
          throw SystemError.conflictingRegistration
        }
        let indexed = IndexedDomain(handle: handle, metadata: domainMetadata)
        rebuilt[domainMetadata.domainID] = indexed
        managed = indexed
      }
      observed[observedID] = IndexedObservation(handle: handle, managed: managed)
    }
    domainsByID = rebuilt
    observationsByID = observed
    orderedObservedIDs = observed.keys.sorted()
    indexed = true
    indexRevision &+= 1
    failureRepairRevision = nil
  }

  private func repairAfterFailure() async throws -> Bool {
    guard failureRepairRevision != indexRevision else { return false }
    try await rebuildIndex()
    failureRepairRevision = indexRevision
    return true
  }

  private func insert(_ indexed: IndexedDomain, observedID: CatalogObservedDomainID) {
    let domainID = indexed.metadata.domainID
    domainsByID[domainID] = indexed
    observationsByID[observedID] = IndexedObservation(handle: indexed.handle, managed: indexed)
    orderedObservedIDs.insert(observedID, at: firstIndex(atOrAfter: observedID))
    indexRevision &+= 1
    failureRepairRevision = nil
  }

  private func removeFromIndex(_ observedID: CatalogObservedDomainID) {
    if let managed = observationsByID[observedID]?.managed {
      domainsByID.removeValue(forKey: managed.metadata.domainID)
    }
    observationsByID.removeValue(forKey: observedID)
    let position = firstIndex(atOrAfter: observedID)
    if position < orderedObservedIDs.count, orderedObservedIDs[position] == observedID {
      orderedObservedIDs.remove(at: position)
    }
    indexRevision &+= 1
    failureRepairRevision = nil
  }

  private func firstIndex(after observedID: CatalogObservedDomainID?) -> Int {
    guard let observedID else { return 0 }
    var low = 0
    var high = orderedObservedIDs.count
    while low < high {
      let middle = low + (high - low) / 2
      if orderedObservedIDs[middle] <= observedID {
        low = middle + 1
      } else {
        high = middle
      }
    }
    return low
  }

  private func firstIndex(atOrAfter observedID: CatalogObservedDomainID) -> Int {
    var low = 0
    var high = orderedObservedIDs.count
    while low < high {
      let middle = low + (high - low) / 2
      if orderedObservedIDs[middle] < observedID {
        low = middle + 1
      } else {
        high = middle
      }
    }
    return low
  }

  private func registered(_ indexed: IndexedDomain) async throws -> CatalogRegisteredDomain {
    let metadata = indexed.metadata
    return try await CatalogRegisteredDomain(
      domainID: metadata.domainID,
      ownerID: metadata.ownerID,
      tenantID: metadata.tenantID,
      generation: metadata.generation,
      rootID: metadata.rootID,
      accessMode: metadata.accessMode,
      presentationInstanceID: metadata.presentationInstanceID,
      displayName: indexed.handle.domain.displayName,
      publicPath: backend.publicPath(for: indexed.handle)
    )
  }

  private func validate(
    _ indexed: IndexedDomain,
    registration: CatalogDomainRegistration
  ) throws {
    let metadata = indexed.metadata
    guard metadata.domainID == registration.domainID,
          metadata.ownerID == registration.ownerID,
          metadata.tenantID == registration.tenantID,
          metadata.generation == registration.generation,
          metadata.rootID == registration.rootID,
          metadata.accessMode == registration.accessMode,
          metadata.presentationInstanceID == registration.presentationInstanceID,
          indexed.handle.domain.displayName == registration.displayName
    else {
      throw SystemError.conflictingRegistration
    }
  }

  private func metadata(_ domain: NSFileProviderDomain) throws -> CatalogDomainMetadata {
    do {
      return try CatalogDomainMetadata(domain: domain)
    } catch CatalogDomainMetadataError.missing {
      throw SystemError.registrationMetadataMissing
    } catch {
      throw SystemError.registrationMismatch
    }
  }
}
