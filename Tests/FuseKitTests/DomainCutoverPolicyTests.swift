@preconcurrency import FileProvider
@testable import FuseKit
import Testing

@Suite("Domain cutover policy")
struct DomainCutoverPolicyTests {
  private struct Fixture {
    let owner: CatalogOwnerID
    let instance: CatalogAccountInstanceID
    let plan: CatalogDomainCutoverPlan
    let registration: CatalogDomainRegistration
  }

  @Test
  func exactLegacyCurrentAndMetadataFreeCurrentAreObserved() throws {
    let fixture = try fixture()
    let legacy = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier("acct-01"),
      displayName: "acct-01"
    )
    #expect(try CatalogDomainCutoverPolicy.observation(
      for: legacy,
      plan: fixture.plan
    )?.legacy == true)

    let current = domain(fixture.registration)
    current.userInfo = CatalogDomainMetadata(registration: fixture.registration).userInfo
    #expect(try CatalogDomainCutoverPolicy.observation(
      for: current,
      plan: fixture.plan
    )?.generation == 7)

    let metadataFree = domain(fixture.registration)
    let observation = try CatalogDomainCutoverPolicy.observation(
      for: metadataFree,
      plan: fixture.plan
    )
    #expect(observation?.legacy == false)
    #expect(observation?.generation == 0)
    #expect(observation?.accountInstanceID == fixture.instance)
  }

  @Test
  func unknownMetadataFreeAndUnplannedOwnerDomainsConflict() throws {
    let fixture = try fixture()
    let unknown = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier("opaque-unplanned-domain"),
      displayName: "Unknown"
    )
    #expect(throws: CatalogDomainController.ControllerError.conflictingDomain) {
      _ = try CatalogDomainCutoverPolicy.observation(for: unknown, plan: fixture.plan)
    }

    let unplannedInstance = try CatalogAccountInstanceID("account-2")
    let unplanned = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(
        ownerID: fixture.owner,
        accountInstanceID: unplannedInstance
      ),
      ownerID: fixture.owner,
      tenantID: CatalogTenantID("tenant-2"),
      generation: 1,
      rootID: rootID(),
      accountInstanceID: unplannedInstance,
      displayName: "Unplanned account"
    )
    let domain = domain(unplanned)
    domain.userInfo = CatalogDomainMetadata(registration: unplanned).userInfo
    #expect(throws: CatalogDomainController.ControllerError.conflictingDomain) {
      _ = try CatalogDomainCutoverPolicy.observation(for: domain, plan: fixture.plan)
    }
  }

  @Test
  func foreignOwnerIsIgnoredAndSpoofedDerivedIdentityFailsClosed() throws {
    let fixture = try fixture()
    let foreignInstance = try CatalogAccountInstanceID("account-2")
    let foreignOwner = try CatalogOwnerID("owner-2")
    let foreign = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: foreignOwner, accountInstanceID: foreignInstance),
      ownerID: foreignOwner,
      tenantID: CatalogTenantID("tenant-foreign"),
      generation: 1,
      rootID: rootID(),
      accountInstanceID: foreignInstance,
      displayName: "Foreign account"
    )
    let foreignDomain = domain(foreign)
    foreignDomain.userInfo = CatalogDomainMetadata(registration: foreign).userInfo
    #expect(try CatalogDomainCutoverPolicy.observation(
      for: foreignDomain,
      plan: fixture.plan
    ) == nil)

    let spoofed = NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(
        CatalogDomainID.derived(ownerID: foreignOwner, accountInstanceID: fixture.instance).rawValue
      ),
      displayName: fixture.registration.displayName
    )
    spoofed.userInfo = CatalogDomainMetadata(registration: fixture.registration).userInfo
    #expect(throws: CatalogDomainMetadataError.mismatch) {
      _ = try CatalogDomainCutoverPolicy.observation(for: spoofed, plan: fixture.plan)
    }
  }

  private func fixture() throws -> Fixture {
    let owner = try CatalogOwnerID("owner-1")
    let instance = try CatalogAccountInstanceID("account-1")
    let plan = try CatalogDomainCutoverPlan(
      operationID: CatalogMutationID("33333333333333333333333333333333"),
      ownerID: owner,
      accounts: [
        CatalogDomainCutoverAccount(
          accountID: 1,
          immutableIdentity: String(repeating: "a", count: 64),
          legacyDomainID: "acct-01",
          accountInstanceID: instance
        ),
      ]
    )
    let registration = try CatalogDomainRegistration(
      domainID: CatalogDomainID.derived(ownerID: owner, accountInstanceID: instance),
      ownerID: owner,
      tenantID: CatalogTenantID("tenant-1"),
      generation: 7,
      rootID: rootID(),
      accountInstanceID: instance,
      displayName: "Account 1"
    )
    return Fixture(owner: owner, instance: instance, plan: plan, registration: registration)
  }

  private func domain(_ registration: CatalogDomainRegistration) -> NSFileProviderDomain {
    NSFileProviderDomain(
      identifier: NSFileProviderDomainIdentifier(registration.domainID.rawValue),
      displayName: registration.displayName
    )
  }
}
