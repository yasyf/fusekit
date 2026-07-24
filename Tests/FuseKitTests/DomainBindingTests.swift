@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

extension DomainControllerTests {
  @Test
  func registeredMetadataRebuildsExactBindingSynchronously() throws {
    let registration = try domainRegistration()
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    domain.userInfo = CatalogDomainMetadata(registration: registration).userInfo

    let binding = try CatalogFileProviderBinding(domain: domain)

    #expect(binding.domainID == registration.domainID)
    #expect(binding.tenant.identifier == registration.tenantID)
    #expect(binding.tenant.generation == registration.generation)
    #expect(binding.rootID == registration.rootID)
    #expect(binding.accessMode == registration.accessMode)
  }

  @Test
  func registeredMetadataRejectsMissingBadAndMismatchedIdentity() throws {
    let registration = try domainRegistration()
    let domain = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
    #expect(throws: CatalogDomainMetadataError.missing) {
      _ = try CatalogFileProviderBinding(domain: domain)
    }
    var bad = CatalogDomainMetadata(registration: registration).userInfo
    let rootKey = try #require(bad.first(where: { $0.value == registration.rootID.rawValue })?.key)
    bad[rootKey] = "bad"
    domain.userInfo = bad
    #expect(throws: Error.self) {
      _ = try CatalogFileProviderBinding(domain: domain)
    }
    var badAccess = CatalogDomainMetadata(registration: registration).userInfo
    let accessKey = try #require(
      badAccess.first(where: { $0.value == registration.accessMode.rawValue })?.key
    )
    badAccess[accessKey] = "unknown"
    domain.userInfo = badAccess
    #expect(throws: CatalogDomainMetadataError.missing) {
      _ = try CatalogFileProviderBinding(domain: domain)
    }
    let mismatched = try NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(
        CatalogDomainID.derived(
          ownerID: CatalogOwnerID("owner-2"),
          presentationInstanceID: CatalogPresentationInstanceID("account-2")
        ).rawValue
      ),
      displayName: registration.displayName
    )
    mismatched.userInfo = CatalogDomainMetadata(registration: registration).userInfo
    #expect(throws: CatalogDomainMetadataError.mismatch) {
      _ = try CatalogFileProviderBinding(domain: mismatched)
    }
  }

  @Test
  func listDomainsUsesStrictBoundedContinuationPages() async throws {
    let system = RecordingDomainSystem()
    let controller = CatalogDomainController(system: system)
    let owner = try CatalogOwnerID("owner-page")
    for index in 0 ... Int(CatalogProtocol.maxBrokerDomainPageSize) {
      let account = try CatalogPresentationInstanceID(String(format: "account-%03d", index))
      _ = try await system.register(
        CatalogDomainRegistration(
          domainID: CatalogDomainID.derived(ownerID: owner, presentationInstanceID: account),
          ownerID: owner,
          tenantID: CatalogTenantID(String(format: "tenant-%03d", index)),
          generation: 1,
          rootID: rootID(),
          accessMode: .readWrite,
          presentationInstanceID: account,
          displayName: "Page"
        )
      )
    }
    let first = try await controller.execute(
      CatalogBrokerCommand(commandID: 1, kind: .listDomains)
    )
    #expect(first.domains?.count == Int(CatalogProtocol.maxBrokerDomainPageSize))
    let cursor = try #require(first.nextAfterObservedID)
    #expect(first.domains?.last?.observedID == cursor)

    let final = try await controller.execute(CatalogBrokerCommand(
      commandID: 2,
      kind: .listDomains,
      afterObservedID: cursor
    ))
    #expect(final.domains?.count == 1)
    #expect(final.nextAfterObservedID == nil)
  }

  @Test
  func commandIdentifiersMustStrictlyIncrease() async throws {
    let controller = CatalogDomainController(system: RecordingDomainSystem())
    let command = try CatalogBrokerCommand(commandID: 1, kind: .listDomains)
    let first = await controller.execute(command)
    let replay = await controller.execute(command)

    #expect(first.code == .ok)
    #expect(replay.code == .invalidRequest)
    #expect(replay.message.contains("increase"))
  }

  @Test
  func registeredDomainRejectsOwnerAccountIdentityMismatch() throws {
    let owner = try CatalogOwnerID("owner-1")
    let account = try CatalogPresentationInstanceID("account-1")
    let domainID = CatalogDomainID.derived(ownerID: owner, presentationInstanceID: account)

    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogRegisteredDomain(
        domainID: domainID,
        ownerID: CatalogOwnerID("owner-2"),
        tenantID: CatalogTenantID("tenant-1"),
        generation: 1,
        rootID: rootID(),
        accessMode: .readWrite,
        presentationInstanceID: account,
        displayName: "Account 1",
        publicPath: "/tmp/account-1"
      )
    }
  }
}
