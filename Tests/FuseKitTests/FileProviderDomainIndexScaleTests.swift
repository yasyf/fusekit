@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

@Suite("File Provider domain index scale")
struct FileProviderDomainIndexScaleTests {
  @Test
  func fourteenRegisteredNineTargetedUseOneFleetScanAndNineSignals() async throws {
    let fixture = try ScaleDomainFixture(count: 14, owner: "owner-fourteen")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let controller = CatalogDomainController(
      system: FileProviderDomainSystem(backend: backend)
    )

    for (offset, registration) in fixture.registrations.prefix(9).enumerated() {
      let result = try await controller.execute(CatalogBrokerCommand(
        commandID: UInt64(offset + 1),
        kind: .signalDomain,
        notification: scaleNotification(registration: registration)
      ))
      #expect(result.code == .ok)
    }

    #expect(await backend.scanCount() == 1)
    #expect(await backend.signalCount() == 9)
    #expect(
      await backend.signaledDomainIDs()
        == Set(fixture.registrations.prefix(9).map(\.domainID))
    )
  }

  @Test
  func hundredRegisteredTenLiveThreeMaterializedSignalExactlyThree() async throws {
    let fixture = try ScaleDomainFixture(count: 100, owner: "owner-hundred")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let controller = CatalogDomainController(
      system: FileProviderDomainSystem(backend: backend)
    )
    let live = Array(fixture.registrations.prefix(10))
    let materialized = Array(live.prefix(3))

    for (offset, registration) in materialized.enumerated() {
      let result = try await controller.execute(CatalogBrokerCommand(
        commandID: UInt64(offset + 1),
        kind: .signalDomain,
        notification: scaleNotification(registration: registration)
      ))
      #expect(result.code == .ok)
    }

    #expect(await backend.scanCount() == 1)
    #expect(await backend.signalCount() == 3)
    #expect(await backend.signaledDomainIDs() == Set(materialized.map(\.domainID)))
    #expect(
      await backend.signaledDomainIDs().isDisjoint(
        with: Set(fixture.registrations.dropFirst(10).map(\.domainID))
      )
    )
  }

  @Test
  func tenThousandDomainsPageFromOneStartupIndex() async throws {
    let fixture = try ScaleDomainFixture(count: 10000, owner: "owner-ten-thousand")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let controller = CatalogDomainController(
      system: FileProviderDomainSystem(backend: backend)
    )
    var commandID: UInt64 = 1
    var after: CatalogObservedDomainID?
    var observed: [CatalogObservedDomainID] = []
    var pageCount = 0

    repeat {
      pageCount += 1
      let result = try await controller.execute(CatalogBrokerCommand(
        commandID: commandID,
        kind: .listDomains,
        afterObservedID: after
      ))
      #expect(result.code == .ok)
      try observed.append(contentsOf: #require(result.domains).map(\.observedID))
      after = result.nextAfterObservedID
      commandID += 1
    } while after != nil

    #expect(observed.count == 10000)
    #expect(observed == observed.sorted { $0.rawValue < $1.rawValue })
    #expect(Set(observed).count == 10000)
    #expect(await backend.scanCount() == 1)
    #expect(await backend.publicPathCount() == 10000 + pageCount - 1)
  }

  @Test
  func repeatedKnownSignalsNeverRescanFleet() async throws {
    let fixture = try ScaleDomainFixture(count: 100, owner: "owner-repeat")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let system = FileProviderDomainSystem(backend: backend)
    let registration = try #require(fixture.registrations.first)
    let target = try CatalogSignalTarget(kind: .workingSet)

    for _ in 0 ..< 1000 {
      try await system.signal(domainID: registration.domainID, targets: [target])
    }

    #expect(await backend.scanCount() == 1)
    #expect(await backend.signalCount() == 1000)
  }

  @Test
  func provisionAndRemoveUpdateKeysetIndexWithoutFleetRescan() async throws {
    let fixture = try ScaleDomainFixture(count: 2, owner: "owner-mutations")
    let backend = ScaleDomainBackend(domains: [])
    let system = FileProviderDomainSystem(backend: backend)

    for registration in fixture.registrations {
      _ = try await system.register(registration)
    }
    let provisioned = try await system.list(after: nil, limit: 10)
    #expect(
      try Set(provisioned.map { try $0.observedID.decodedIdentifier() })
        == Set(fixture.registrations.map(\.domainID.rawValue))
    )
    #expect(await backend.scanCount() == 1)

    let removed = try #require(fixture.registrations.first).domainID
    #expect(try await system.remove(CatalogObservedDomainID(observing: removed.rawValue)))
    let remaining = try await system.list(after: nil, limit: 10)
    #expect(
      try remaining.map { try $0.observedID.decodedIdentifier() }
        == fixture.registrations.map(\.domainID.rawValue).filter { $0 != removed.rawValue }
    )
    #expect(await backend.scanCount() == 1)
  }

  @Test
  func failedStaleHandleRepairsOnceAndRestartRebuildsExactMetadata() async throws {
    let fixture = try ScaleDomainFixture(count: 1, owner: "owner-drift")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let registration = try #require(fixture.registrations.first)
    let system = FileProviderDomainSystem(backend: backend)
    let binding = CatalogBrokerBindDomainRequest(
      domainID: registration.domainID,
      tenantID: registration.tenantID,
      generation: registration.generation
    )
    try await system.validate(binding)
    #expect(await backend.scanCount() == 1)

    await backend.removeExternally(registration.domainID)
    await #expect(throws: ScaleDomainBackend.BackendError.staleHandle) {
      try await system.signal(
        domainID: registration.domainID,
        targets: [CatalogSignalTarget(kind: .workingSet)]
      )
    }
    #expect(await backend.scanCount() == 2)
    await #expect(throws: FileProviderDomainSystem.SystemError.domainNotFound) {
      try await system.signal(
        domainID: registration.domainID,
        targets: [CatalogSignalTarget(kind: .workingSet)]
      )
    }
    #expect(await backend.scanCount() == 2)

    let replacement = try CatalogDomainRegistration(
      domainID: registration.domainID,
      ownerID: registration.ownerID,
      tenantID: registration.tenantID,
      generation: registration.generation + 1,
      rootID: registration.rootID,
      accessMode: registration.accessMode,
      presentationInstanceID: registration.presentationInstanceID,
      displayName: registration.displayName
    )
    try await backend.insertExternally(domainHandle(for: replacement))
    let restarted = FileProviderDomainSystem(backend: backend)
    try await restarted.validate(
      CatalogBrokerBindDomainRequest(
        domainID: replacement.domainID,
        tenantID: replacement.tenantID,
        generation: replacement.generation
      )
    )
    await #expect(throws: FileProviderDomainSystem.SystemError.registrationMismatch) {
      try await restarted.validate(binding)
    }
    #expect(await backend.scanCount() == 3)
  }

  @Test
  func registerRecreatesExternallyLostDomainBeforeReturningPublicPath() async throws {
    let fixture = try ScaleDomainFixture(count: 1, owner: "owner-lost")
    let backend = ScaleDomainBackend(domains: fixture.handles)
    let registration = try #require(fixture.registrations.first)
    let system = FileProviderDomainSystem(backend: backend)

    _ = try await system.register(registration)
    await backend.removeExternally(registration.domainID)

    let restored = try await system.register(registration)
    #expect(restored.domainID == registration.domainID)
    #expect(restored.publicPath == "/public/\(registration.domainID.rawValue)")
    #expect(await backend.scanCount() == 2)
  }

  @Test
  func metadataFreeAndMalformedDomainsAreRemovalOnlyObservations() async throws {
    let registration = try #require(
      ScaleDomainFixture(count: 1, owner: "owner-legacy").registrations.first
    )
    let legacy = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier("legacy/account\\name\n"),
      displayName: "Legacy"
    )
    let malformed = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    malformed.userInfo = ["fusekit.tenant_id": registration.tenantID.rawValue]
    let backend = ScaleDomainBackend(
      domains: [
        FileProviderDomainHandle(domain: legacy), FileProviderDomainHandle(domain: malformed),
      ]
    )
    let system = FileProviderDomainSystem(backend: backend)

    let observed = try await system.list(after: nil, limit: 10)
    #expect(
      try observed.map { try $0.observedID.decodedIdentifier() }
        == [
          registration.domainID.rawValue, "legacy/account\\name\n",
        ].sorted()
    )
    #expect(observed.allSatisfy { $0.managed == nil })
    #expect(await backend.publicPathCount() == 0)
    await #expect(throws: FileProviderDomainSystem.SystemError.conflictingRegistration) {
      _ = try await system.register(registration)
    }

    for domain in observed {
      #expect(try await system.remove(domain.observedID))
    }
    #expect(try await system.list(after: nil, limit: 10).isEmpty)
    let registered = try await system.register(registration)
    #expect(registered.domainID == registration.domainID)
  }
}

private struct ScaleDomainFixture {
  let registrations: [CatalogDomainRegistration]
  let handles: [FileProviderDomainHandle]

  init(count: Int, owner: String) throws {
    let ownerID = try CatalogOwnerID(owner)
    var registrations: [CatalogDomainRegistration] = []
    var handles: [FileProviderDomainHandle] = []
    registrations.reserveCapacity(count)
    handles.reserveCapacity(count)
    for index in 0 ..< count {
      let account = try CatalogPresentationInstanceID(String(format: "account-%05d", index))
      let registration = try CatalogDomainRegistration(
        domainID: CatalogDomainID.derived(ownerID: ownerID, presentationInstanceID: account),
        ownerID: ownerID,
        tenantID: CatalogTenantID(String(format: "tenant-%05d", index)),
        generation: 1,
        rootID: rootID(),
        accessMode: .readWrite,
        presentationInstanceID: account,
        displayName: String(format: "Account %05d", index)
      )
      registrations.append(registration)
      try handles.append(domainHandle(for: registration))
    }
    self.registrations = registrations
    self.handles = handles
  }
}

private func domainHandle(
  for registration: CatalogDomainRegistration
) throws -> FileProviderDomainHandle {
  let domain = NSFileProviderDomain(
    identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
    displayName: registration.displayName
  )
  domain.userInfo = CatalogDomainMetadata(registration: registration).userInfo
  return FileProviderDomainHandle(domain: domain)
}

private func scaleNotification(
  registration: CatalogDomainRegistration
) throws -> CatalogActivationNotification {
  let targets = try [CatalogSignalTarget(kind: .workingSet)]
  return try testActivationNotification(
    tenantID: registration.tenantID, domainID: registration.domainID,
    generation: registration.generation, activationRevision: 1, catalogHead: 1,
    targetCount: 1,
    targets: targets
  )
}

private actor ScaleDomainBackend: FileProviderDomainBackend {
  enum BackendError: Error, Equatable {
    case duplicate
    case staleHandle
  }

  private var registered: [String: FileProviderDomainHandle]
  private var scans = 0
  private var signalBatches: [CatalogDomainID] = []
  private var pathLookups = 0

  init(domains: [FileProviderDomainHandle]) {
    registered = Dictionary(uniqueKeysWithValues: domains.map { ($0.identifier, $0) })
  }

  func domains() async throws -> [FileProviderDomainHandle] {
    scans += 1
    return Array(registered.values)
  }

  func add(_ domain: FileProviderDomainHandle) async throws {
    guard registered[domain.identifier] == nil else { throw BackendError.duplicate }
    registered[domain.identifier] = domain
  }

  func remove(_ domain: FileProviderDomainHandle) async throws {
    guard let current = registered[domain.identifier],
          current.domain === domain.domain
    else { throw BackendError.staleHandle }
    registered.removeValue(forKey: domain.identifier)
  }

  func publicPath(for domain: FileProviderDomainHandle) async throws -> String {
    guard let current = registered[domain.identifier],
          current.domain === domain.domain
    else { throw BackendError.staleHandle }
    pathLookups += 1
    return "/public/\(domain.identifier)"
  }

  func signal(
    domain: FileProviderDomainHandle,
    targets _: [CatalogSignalTarget]
  ) async throws {
    guard let current = registered[domain.identifier],
          current.domain === domain.domain
    else { throw BackendError.staleHandle }
    try signalBatches.append(CatalogDomainID(domain.identifier))
  }

  func materializeCritical(
    domain: FileProviderDomainHandle,
    objects: [CatalogResolvedCriticalObjectProof]
  ) async throws -> [CatalogCriticalMaterializationPath] {
    guard let current = registered[domain.identifier], current.domain === domain.domain else {
      throw BackendError.staleHandle
    }
    return try objects.sorted { $0.objectID.rawValue < $1.objectID.rawValue }.map {
      try CatalogCriticalMaterializationPath(
        objectID: $0.objectID,
        path: "/public/\(domain.identifier)/\($0.objectID.rawValue)"
      )
    }
  }

  func removeExternally(_ domainID: CatalogDomainID) {
    registered.removeValue(forKey: domainID.rawValue)
  }

  func insertExternally(_ domain: FileProviderDomainHandle) {
    registered[domain.identifier] = domain
  }

  func scanCount() -> Int {
    scans
  }

  func signalCount() -> Int {
    signalBatches.count
  }

  func signaledDomainIDs() -> Set<CatalogDomainID> {
    Set(signalBatches)
  }

  func publicPathCount() -> Int {
    pathLookups
  }
}
