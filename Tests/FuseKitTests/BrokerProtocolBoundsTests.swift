import Foundation
@testable import FuseKit
import Testing

@Suite("Broker protocol structural bounds")
struct BrokerProtocolBoundsTests {
  @Test
  func forwardPayloadUsesExactByteBounds() throws {
    let binding = try CatalogBrokerForwardContext(
      domainID: domainID(account: "account-1"),
      tenantID: CatalogTenantID("tenant-1"),
      generation: 1
    )
    _ = try CatalogBrokerForwardRequest(
      context: binding,
      operation: .catalogHead,
      payload: Data(repeating: 1, count: Int(CatalogProtocol.maxBrokerForwardPayloadBytes))
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogBrokerForwardRequest(
        context: binding,
        operation: .catalogHead,
        payload: Data(
          repeating: 1,
          count: Int(CatalogProtocol.maxBrokerForwardPayloadBytes) + 1
        )
      )
    }
  }

  @Test
  func domainMetadataAndPagesUseExactBounds() throws {
    let prefix = "/Users/test/Library/CloudStorage/"
    let exactPath =
      prefix
        + String(
          repeating: "p",
          count: Int(CatalogProtocol.maxPublicPathBytes) - prefix.utf8.count
        )
    let domains = try boundedDomains(publicPath: exactPath)
    try expectDomainBounds(domains: domains, exactPath: exactPath)
  }

  @Test
  func brokerErrorMessageUsesExactBound() throws {
    _ = try CatalogBrokerResult(
      code: .unavailable,
      message: String(repeating: "e", count: Int(CatalogProtocol.maxErrorMessageBytes)),
      commandID: 1,
      kind: .listDomains
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogBrokerResult(
        code: .unavailable,
        message: String(repeating: "e", count: Int(CatalogProtocol.maxErrorMessageBytes) + 1),
        commandID: 1,
        kind: .listDomains
      )
    }
  }

  private func expectDomainBounds(
    domains: [CatalogRegisteredDomain],
    exactPath: String
  ) throws {
    let exact = Array(domains.prefix(Int(CatalogProtocol.maxBrokerDomainPageSize)))
    _ = try CatalogBrokerResult(
      code: .ok,
      message: "",
      commandID: 1,
      kind: .listDomains,
      domains: exact,
      nextAfterDomainID: exact.last?.domainID
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogBrokerResult(
        code: .ok,
        message: "",
        commandID: 1,
        kind: .listDomains,
        domains: domains
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogRegisteredDomain(
        domainID: domains[0].domainID,
        ownerID: domains[0].ownerID,
        tenantID: domains[0].tenantID,
        generation: domains[0].generation,
        rootID: domains[0].rootID,
        accessMode: domains[0].accessMode,
        presentationInstanceID: domains[0].presentationInstanceID,
        displayName: domains[0].displayName,
        publicPath: exactPath + "x"
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogDomainRegistration(
        domainID: domains[0].domainID,
        ownerID: domains[0].ownerID,
        tenantID: domains[0].tenantID,
        generation: domains[0].generation,
        rootID: domains[0].rootID,
        accessMode: domains[0].accessMode,
        presentationInstanceID: domains[0].presentationInstanceID,
        displayName: String(
          repeating: "d",
          count: Int(CatalogProtocol.maxDisplayNameBytes) + 1
        )
      )
    }
  }

  @Test
  func everyDecodedResponseUsesTheExactErrorMessageInvariant() throws {
    let decoder = JSONDecoder()
    let exact = CatalogBrokerBindDomainResponse(
      code: .unavailable,
      message: String(repeating: "e", count: Int(CatalogProtocol.maxErrorMessageBytes))
    )
    _ = try decoder.decode(
      CatalogBrokerBindDomainResponse.self,
      from: JSONEncoder().encode(exact)
    )

    for response in [
      CatalogBrokerBindDomainResponse(
        code: .unavailable,
        message: String(
          repeating: "e",
          count: Int(CatalogProtocol.maxErrorMessageBytes) + 1
        )
      ),
      CatalogBrokerBindDomainResponse(code: .unavailable, message: ""),
      CatalogBrokerBindDomainResponse(code: .ok, message: "unexpected"),
    ] {
      let payload = try JSONEncoder().encode(response)
      #expect(throws: CatalogProtocolCodingError.self) {
        _ = try decoder.decode(CatalogBrokerBindDomainResponse.self, from: payload)
      }
    }
  }

  @Test
  func mutationRequestCommitAndCausalIdentitiesStayDistinct() throws {
    _ = try CatalogMutationRequestID("11111111111111111111111111111111")
    _ = try CatalogOperationID("22222222222222222222222222222222")
    _ = try CatalogMutationID(
      "0000000000000003333333333333333333333333333333333333333333333333"
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogMutationRequestID(
        "0000000000000003333333333333333333333333333333333333333333333333"
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogOperationID(
        "0000000000000003333333333333333333333333333333333333333333333333"
      )
    }
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogMutationID("33333333333333333333333333333333")
    }
  }

  @Test
  func desiredSourceFleetUsesExactSnapshotAndDriverConfigBounds() throws {
    let declaration = try CatalogSourceAuthorityDeclaration(
      authority: CatalogSourceAuthorityID("authority-a"),
      driverID: "driver.v1",
      driverConfig: Data(repeating: 1, count: Int(CatalogProtocol.maxSourceDriverConfigBytes)),
      declarationDigest: String(repeating: "a", count: 64)
    )
    _ = try CatalogPublishDesiredSourceFleetRequest(
      owner: "owner",
      expectedGeneration: 0,
      generation: 1,
      declarations: [declaration]
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogSourceAuthorityDeclaration(
        authority: CatalogSourceAuthorityID("authority-a"),
        driverID: "driver.v1",
        driverConfig: Data(
          repeating: 1,
          count: Int(CatalogProtocol.maxSourceDriverConfigBytes) + 1
        ),
        declarationDigest: String(repeating: "a", count: 64)
      )
    }
    _ = try CatalogReadDesiredSourceFleetRequest(owner: "owner", generation: 0, limit: 16)
    let digest = String(repeating: "d", count: 64)
    _ = try CatalogReadDesiredSourceFleetRequest(
      owner: "owner",
      generation: 1,
      snapshotDigest: digest,
      after: CatalogSourceAuthorityID("authority-a"),
      limit: 16
    )
    #expect(throws: CatalogProtocolCodingError.self) {
      _ = try CatalogReadDesiredSourceFleetRequest(
        owner: "owner",
        generation: 1,
        limit: 16
      )
    }
  }

  private func boundedDomains(publicPath: String) throws -> [CatalogRegisteredDomain] {
    var domains: [CatalogRegisteredDomain] = []
    for index in 0 ... Int(CatalogProtocol.maxBrokerDomainPageSize) {
      let account = try CatalogPresentationInstanceID(String(format: "account-%03d", index))
      try domains.append(
        CatalogRegisteredDomain(
          domainID: CatalogDomainID.derived(
            ownerID: CatalogOwnerID("owner-1"),
            presentationInstanceID: account
          ),
          ownerID: CatalogOwnerID("owner-1"),
          tenantID: CatalogTenantID(String(format: "tenant-%03d", index)),
          generation: 1,
          rootID: CatalogObjectID("00000000000000000000000000000001"),
          accessMode: .readWrite,
          presentationInstanceID: account,
          displayName: String(repeating: "d", count: Int(CatalogProtocol.maxDisplayNameBytes)),
          publicPath: publicPath
        )
      )
    }
    return domains.sorted { $0.domainID.rawValue < $1.domainID.rawValue }
  }

  private func domainID(account: String) throws -> CatalogDomainID {
    try CatalogDomainID.derived(
      ownerID: CatalogOwnerID("owner-1"),
      presentationInstanceID: CatalogPresentationInstanceID(account)
    )
  }
}
