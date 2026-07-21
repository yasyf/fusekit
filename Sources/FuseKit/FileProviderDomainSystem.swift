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

  private let backend: any FileProviderDomainBackend
  private var indexed = false
  private var domainsByID: [CatalogDomainID: IndexedDomain] = [:]
  private var orderedIDs: [CatalogDomainID] = []
  private var indexRevision: UInt64 = 0
  private var failureRepairRevision: UInt64?

  init(backend: any FileProviderDomainBackend = NativeFileProviderDomainBackend()) {
    self.backend = backend
  }

  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain {
    guard registration.generation > 0 else { throw SystemError.conflictingRegistration }
    try await ensureIndex()
    if let existing = domainsByID[registration.domainID] {
      try validate(existing, registration: registration)
      do {
        return try await registered(existing)
      } catch {
        if try await repairAfterFailure(), let repaired = domainsByID[registration.domainID] {
          try validate(repaired, registration: registration)
          return try await registered(repaired)
        }
        throw error
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
    insert(entry)
    return try await registered(entry)
  }

  func remove(_ domainID: CatalogDomainID) async throws -> Bool {
    try await ensureIndex()
    guard let existing = domainsByID[domainID] else { return true }
    do {
      try await backend.remove(existing.handle)
    } catch {
      if try await repairAfterFailure(), domainsByID[domainID] == nil {
        return true
      }
      throw error
    }
    removeFromIndex(domainID)
    return true
  }

  func list(after: CatalogDomainID?, limit: Int) async throws -> [CatalogRegisteredDomain] {
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
    after: CatalogDomainID?,
    limit: Int
  ) async throws -> [CatalogRegisteredDomain] {
    let start = firstIndex(after: after)
    let end = min(orderedIDs.count, start + limit + 1)
    var result: [CatalogRegisteredDomain] = []
    result.reserveCapacity(end - start)
    for domainID in orderedIDs[start ..< end] {
      guard let indexed = domainsByID[domainID] else {
        throw SystemError.registrationMismatch
      }
      try await result.append(registered(indexed))
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
    for handle in try await backend.domains() {
      guard CatalogDomainMetadata.declaresMetadata(handle.domain) else { continue }
      let domainMetadata = try metadata(handle.domain)
      guard rebuilt[domainMetadata.domainID] == nil else {
        throw SystemError.conflictingRegistration
      }
      rebuilt[domainMetadata.domainID] = IndexedDomain(
        handle: handle,
        metadata: domainMetadata
      )
    }
    domainsByID = rebuilt
    orderedIDs = rebuilt.keys.sorted { $0.rawValue < $1.rawValue }
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

  private func insert(_ indexed: IndexedDomain) {
    let domainID = indexed.metadata.domainID
    domainsByID[domainID] = indexed
    orderedIDs.insert(domainID, at: firstIndex(atOrAfter: domainID))
    indexRevision &+= 1
    failureRepairRevision = nil
  }

  private func removeFromIndex(_ domainID: CatalogDomainID) {
    domainsByID.removeValue(forKey: domainID)
    let position = firstIndex(atOrAfter: domainID)
    if position < orderedIDs.count, orderedIDs[position] == domainID {
      orderedIDs.remove(at: position)
    }
    indexRevision &+= 1
    failureRepairRevision = nil
  }

  private func firstIndex(after domainID: CatalogDomainID?) -> Int {
    guard let domainID else { return 0 }
    var low = 0
    var high = orderedIDs.count
    while low < high {
      let middle = low + (high - low) / 2
      if orderedIDs[middle].rawValue <= domainID.rawValue {
        low = middle + 1
      } else {
        high = middle
      }
    }
    return low
  }

  private func firstIndex(atOrAfter domainID: CatalogDomainID) -> Int {
    var low = 0
    var high = orderedIDs.count
    while low < high {
      let middle = low + (high - low) / 2
      if orderedIDs[middle].rawValue < domainID.rawValue {
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
      accountInstanceID: metadata.accountInstanceID,
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
          metadata.accountInstanceID == registration.accountInstanceID,
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
