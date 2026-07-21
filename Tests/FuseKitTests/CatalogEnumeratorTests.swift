@preconcurrency import FileProvider
import Foundation
@testable import FuseKit
import Testing

@Suite("Catalog change enumeration")
struct CatalogEnumeratorTests {
  @Test
  func observerFinishesBeforeExactAcknowledgementAndTaskDrains() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(recorder: recorder, failAcknowledgement: false)
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: fixture.enumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()

    #expect(recorder.values() == ["finish", "ack"])
    #expect(observer.errors() == 0)
    #expect(await fixture.transport.acknowledgements() == [7])
  }

  @Test
  func acknowledgementFailureAfterFinishDoesNotSendSecondObserverTerminal() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(recorder: recorder, failAcknowledgement: true)
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: fixture.enumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()

    #expect(recorder.values() == ["finish", "ack"])
    #expect(observer.finishes() == 1)
    #expect(observer.errors() == 0)
    await #expect(
      throws: CatalogConvergenceInbox.InboxError.notificationStreamFailed("acknowledgement")
    ) {
      try await fixture.inbox.checkHealthy()
    }
  }

  @Test
  func oneRevisionPaginatesBySequenceWithoutEarlyAcknowledgement() async throws {
    let recorder = OrderingRecorder()
    let fixture = try EnumeratorFixture(
      recorder: recorder,
      failAcknowledgement: false,
      paginated: true
    )
    try await fixture.inbox.receive(fixture.notification)
    let observer = RecordingChangeObserver(recorder: recorder)

    fixture.enumerator.enumerateChanges(
      for: observer,
      from: fixture.enumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await fixture.waitUntilDrained()
    #expect(await fixture.transport.acknowledgements().isEmpty)
    #expect(observer.moreComingValues() == [true])

    let next = try #require(observer.lastAnchor())
    fixture.enumerator.enumerateChanges(for: observer, from: next)
    await fixture.waitUntilDrained()

    #expect(observer.updateCount() == 2)
    #expect(observer.moreComingValues() == [true, false])
    #expect(await fixture.transport.requestedCursors() == ["6:4294967295", "7:1"])
    #expect(await fixture.transport.acknowledgements() == [7])
    #expect(recorder.values() == ["finish", "finish", "ack"])
  }

  @Test
  func anchorReplayAcrossTenantGenerationExpiresBeforeDeltaRead() async throws {
    let source = try EnumeratorFixture(
      recorder: OrderingRecorder(),
      failAcknowledgement: false,
      generation: 3
    )
    let target = try EnumeratorFixture(
      recorder: OrderingRecorder(),
      failAcknowledgement: false,
      generation: 4
    )
    let observer = RecordingChangeObserver(recorder: OrderingRecorder())

    target.enumerator.enumerateChanges(
      for: observer,
      from: source.enumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await target.waitUntilDrained()

    #expect(observer.errorCodes() == [NSFileProviderError.Code.syncAnchorExpired.rawValue])
    #expect(await target.transport.requestedCursors().isEmpty)
  }

  @Test
  func anchorReplayAcrossEnumerationScopeExpiresBeforeDeltaRead() async throws {
    let source = try EnumeratorFixture(
      recorder: OrderingRecorder(),
      failAcknowledgement: false,
      scope: .workingSet
    )
    let target = try EnumeratorFixture(
      recorder: OrderingRecorder(),
      failAcknowledgement: false,
      scope: .container(CatalogObjectID("cccccccccccccccccccccccccccccccc"))
    )
    let observer = RecordingChangeObserver(recorder: OrderingRecorder())

    target.enumerator.enumerateChanges(
      for: observer,
      from: source.enumerator.anchor(
        CatalogChangeCursor(
          revision: 6,
          sequence: CatalogProtocol.changeCursorCompleteSequence
        )
      )
    )
    await target.waitUntilDrained()

    #expect(observer.errorCodes() == [NSFileProviderError.Code.syncAnchorExpired.rawValue])
    #expect(await target.transport.requestedCursors().isEmpty)
  }

  @Test
  func anchorReplayAcrossDomainTenantOrRootExpiresBeforeDeltaRead() async throws {
    let source = try EnumeratorFixture(
      recorder: OrderingRecorder(),
      failAcknowledgement: false
    )
    let targets = try [
      EnumeratorFixture(
        recorder: OrderingRecorder(),
        failAcknowledgement: false,
        owner: "owner-2",
        account: "account-2"
      ),
      EnumeratorFixture(
        recorder: OrderingRecorder(),
        failAcknowledgement: false,
        tenantID: "tenant-2"
      ),
      EnumeratorFixture(
        recorder: OrderingRecorder(),
        failAcknowledgement: false,
        rootID: "dddddddddddddddddddddddddddddddd"
      ),
    ]
    let anchor = source.enumerator.anchor(
      CatalogChangeCursor(
        revision: 6,
        sequence: CatalogProtocol.changeCursorCompleteSequence
      )
    )

    for target in targets {
      let observer = RecordingChangeObserver(recorder: OrderingRecorder())
      target.enumerator.enumerateChanges(for: observer, from: anchor)
      await target.waitUntilDrained()
      #expect(observer.errorCodes() == [NSFileProviderError.Code.syncAnchorExpired.rawValue])
      #expect(await target.transport.requestedCursors().isEmpty)
    }
  }
}

enum CatalogEnumeratorTestError: Error, Equatable {
  case acknowledgement
}
