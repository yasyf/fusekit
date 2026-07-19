@preconcurrency import FileProvider

final class FileProviderDomainSystem: CatalogDomainSystem, @unchecked Sendable {
  private enum SystemError: Error {
    case conflictingRegistration
    case domainNotFound
    case invalidTarget
    case registrationMetadataMissing
    case registrationMismatch
    case cutoverConflict
  }

  func register(_ registration: CatalogDomainRegistration) async throws -> CatalogRegisteredDomain {
    guard registration.generation > 0 else { throw SystemError.conflictingRegistration }
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == registration.domainID.rawValue
    }
    guard matches.count <= 1 else { throw SystemError.conflictingRegistration }
    if let existing = matches.first {
      let metadata = try metadata(existing)
      guard metadata.domainID == registration.domainID,
            metadata.ownerID == registration.ownerID,
            metadata.tenantID == registration.tenantID,
            metadata.generation == registration.generation,
            metadata.rootID == registration.rootID,
            metadata.accountInstanceID == registration.accountInstanceID,
            existing.displayName == registration.displayName
      else {
        throw SystemError.conflictingRegistration
      }
      return try await registered(existing)
    }
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    domain.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    try await NSFileProviderManager.add(domain)
    return try await registered(domain)
  }

  func remove(_ domainID: CatalogDomainID) async throws -> Bool {
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == domainID.rawValue
    }
    for domain in matches {
      try await NSFileProviderManager.remove(domain)
    }
    return try await !NSFileProviderManager.domains().contains {
      $0.identifier.rawValue == domainID.rawValue
    }
  }

  func list() async throws -> [CatalogRegisteredDomain] {
    var result: [CatalogRegisteredDomain] = []
    for domain in try await NSFileProviderManager.domains()
      where CatalogDomainMetadata.declaresMetadata(domain) {
      try await result.append(registered(domain))
    }
    return result
  }

  func validate(_ binding: CatalogBrokerBindDomainRequest) async throws {
    let matches = try await NSFileProviderManager.domains().filter {
      $0.identifier.rawValue == binding.domainID.rawValue
    }
    guard matches.count == 1, let domain = matches.first else {
      throw SystemError.domainNotFound
    }
    let metadata = try metadata(domain)
    guard metadata.domainID == binding.domainID,
          metadata.tenantID == binding.tenantID,
          metadata.generation == binding.generation
    else {
      throw SystemError.registrationMismatch
    }
  }

  func signal(domainID: CatalogDomainID, target: CatalogSignalTarget) async throws {
    guard
      let domain = try await NSFileProviderManager.domains().first(where: {
        $0.identifier.rawValue == domainID.rawValue
      }), let manager = NSFileProviderManager(for: domain)
    else {
      throw SystemError.domainNotFound
    }
    let identifier: NSFileProviderItemIdentifier
    switch target.kind {
    case .workingSet:
      identifier = .workingSet
    case .container:
      guard let parentID = target.parentID else { throw SystemError.invalidTarget }
      identifier = NSFileProviderItemIdentifier(parentID.rawValue)
    }
    try await manager.signalEnumerator(for: identifier)
  }

  func cutover(_ plan: CatalogDomainCutoverPlan) async throws -> [CatalogDomainCutoverObservation] {
    let actual = try await NSFileProviderManager.domains()
    var removals: [NSFileProviderDomain] = []
    var observations: [CatalogDomainCutoverObservation] = []
    var observedIDs: Set<String> = []
    for domain in actual {
      guard let observation = try CatalogDomainCutoverPolicy.observation(for: domain, plan: plan)
      else { continue }
      guard observedIDs.insert(observation.domainID).inserted else {
        throw SystemError.cutoverConflict
      }
      removals.append(domain)
      observations.append(observation)
    }
    for domain in removals {
      try await NSFileProviderManager.remove(domain)
    }
    let remaining = try await NSFileProviderManager.domains()
    guard try remaining.allSatisfy({
      try CatalogDomainCutoverPolicy.observation(for: $0, plan: plan) == nil
    }) else {
      throw SystemError.cutoverConflict
    }
    return observations.sorted { $0.domainID < $1.domainID }
  }

  private func registered(_ domain: NSFileProviderDomain) async throws -> CatalogRegisteredDomain {
    let metadata = try metadata(domain)
    guard let manager = NSFileProviderManager(for: domain) else {
      throw SystemError.registrationMetadataMissing
    }
    let url = try await manager.getUserVisibleURL(for: .rootContainer)
    return try CatalogRegisteredDomain(
      domainID: metadata.domainID,
      ownerID: metadata.ownerID,
      tenantID: metadata.tenantID,
      generation: metadata.generation,
      rootID: metadata.rootID,
      accountInstanceID: metadata.accountInstanceID,
      displayName: domain.displayName,
      publicPath: url.path
    )
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
