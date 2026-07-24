import Foundation
import Testing

@testable import FuseKit

@Suite("Bounded convergence notifications")
struct ConvergenceSummaryTests {
  @Test
  func largeChangeAndTargetSetsUseConstantSizeSummaries() throws {
    let notification = try summarizedNotification(
      targetCount: 10000,
      targetsCoalesced: true,
      targets: [CatalogSignalTarget(kind: .workingSet)]
    )
    try CatalogActivationInbox.validatePayload(notification)
  }

  @Test
  func malformedSummariesAndCoarseTargetsFailClosed() throws {
    let badDigest = try summarizedNotification(affectedDigest: String(repeating: "A", count: 64))
    #expect(throws: CatalogActivationInbox.InboxError.invalidAffectedSummary) {
      try CatalogActivationInbox.validatePayload(badDigest)
    }

    let parent = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let badCoarseTarget = try summarizedNotification(
      targetCount: 65,
      targetsCoalesced: true,
      targets: [CatalogSignalTarget(kind: .container, parentID: parent)]
    )
    #expect(throws: CatalogActivationInbox.InboxError.invalidTargets) {
      try CatalogActivationInbox.validatePayload(badCoarseTarget)
    }
  }

  @Test
  func unsolicitedContainerObservationsStayHardBounded() async throws {
    let binding = try CatalogFileProviderBinding(
      domainID: domainID(),
      tenant: CatalogTenant(identifier: CatalogTenantID("tenant-1"), generation: 7),
      rootID: CatalogObjectID("ffffffffffffffffffffffffffffffff"),
      accessMode: .readWrite
    )
    let inbox = CatalogActivationInbox(
      binding: binding,
      client: CatalogClient(transport: AckTransport())
    )

    for index in 0..<1000 {
      try await inbox.acknowledgeObserved(
        target: CatalogSignalTarget(
          kind: .container,
          parentID: CatalogObjectID(String(format: "%032x", index + 1))
        ),
        upTo: 1
      )
    }

    #expect(await inbox.observedTargetCount() == Int(CatalogProtocol.maxSignalTargets))
  }

  private func summarizedNotification(
    affectedDigest: String = String(repeating: "a", count: 64),
    targetCount: UInt64 = 1,
    targetsCoalesced: Bool = false,
    targets: [CatalogSignalTarget]? = nil
  ) throws -> CatalogActivationNotification {
    let signalTargets = try targets ?? [CatalogSignalTarget(kind: .workingSet)]
    return try testActivationNotification(
      tenantID: CatalogTenantID("tenant-1"), domainID: domainID(), generation: 7,
      activationRevision: 9, catalogHead: 8, sourceRevision: 4,
      affectedKeysDigest: affectedDigest,
      targetCount: targetCount,
      targetsCoalesced: targetsCoalesced,
      targets: signalTargets
    )
  }
}
