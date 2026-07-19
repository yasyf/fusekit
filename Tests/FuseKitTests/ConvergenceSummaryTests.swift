import Foundation
import Testing

@testable import FuseKit

@Suite("Bounded convergence notifications")
struct ConvergenceSummaryTests {
  @Test
  func largeChangeAndTargetSetsUseConstantSizeSummaries() throws {
    let notification = try summarizedNotification(
      affectedCount: 10_000,
      targetCount: 10_000,
      targetsCoalesced: true,
      targets: [CatalogSignalTarget(kind: .workingSet)]
    )
    try CatalogConvergenceInbox.validatePayload(notification)
  }

  @Test
  func malformedSummariesAndCoarseTargetsFailClosed() throws {
    let badDigest = try summarizedNotification(affectedDigest: String(repeating: "A", count: 64))
    #expect(throws: CatalogConvergenceInbox.InboxError.invalidAffectedSummary) {
      try CatalogConvergenceInbox.validatePayload(badDigest)
    }

    let parent = try CatalogObjectID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    let badCoarseTarget = try summarizedNotification(
      targetCount: 65,
      targetsCoalesced: true,
      targets: [CatalogSignalTarget(kind: .container, parentID: parent)]
    )
    #expect(throws: CatalogConvergenceInbox.InboxError.invalidTargets) {
      try CatalogConvergenceInbox.validatePayload(badCoarseTarget)
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
    let inbox = CatalogConvergenceInbox(
      binding: binding,
      client: CatalogClient(transport: AckTransport())
    )

    for index in 0..<1_000 {
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
    affectedCount: UInt64 = 1,
    affectedDigest: String = String(repeating: "a", count: 64),
    targetCount: UInt64 = 1,
    targetsCoalesced: Bool = false,
    targets: [CatalogSignalTarget]? = nil
  ) throws -> CatalogConvergenceNotification {
    let signalTargets = try targets ?? [CatalogSignalTarget(kind: .workingSet)]
    return try CatalogConvergenceNotification(
      tenantID: CatalogTenantID("tenant-1"),
      domainID: domainID(),
      generation: 7,
      revision: 9,
      catalogRevision: 8,
      sourceAuthority: CatalogSourceAuthorityID("source-main"),
      sourceRevision: 4,
      changeID: CatalogChangeID("11111111111111111111111111111111"),
      operationID: CatalogOperationID("22222222222222222222222222222222"),
      cause: .daemonWrite,
      originGeneration: 0,
      fingerprint: String(repeating: "c", count: 64),
      affectedCount: affectedCount,
      affectedDigest: affectedDigest,
      targetCount: targetCount,
      targetDigest: String(repeating: "b", count: 64),
      targetsCoalesced: targetsCoalesced,
      targets: signalTargets
    )
  }
}
