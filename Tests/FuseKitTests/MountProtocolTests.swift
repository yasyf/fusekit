import Foundation
@testable import FuseKit
import Testing

@Suite("Mount protocol")
struct MountProtocolTests {
  @Test
  func runtimeHealthResponseBoundIsProtocolOwned() {
    #expect(MountProtocol.runtimeHealthMaxResponseBytes == 16 * 1024)
  }

  @Test
  func runtimeHealthRoundTripsExactReadinessProof() throws {
    let proof = try MountNativeMountProof(
      presentationRoot: "/Volumes/FuseKit",
      filesystem: MountProtocol.nativeMountFilesystem,
      source: "fuse-t:/FuseKit",
      rootReadEpoch: 7
    )
    let health = try MountRuntimeHealthResponse(
      code: .ok,
      message: "",
      runtimeBuild: "product-1.8.0",
      runtimeProtocol: MountProtocol.runtimeProtocolVersion,
      runtimePID: 4242,
      processGeneration: "process-7",
      activationGeneration: "activation-7",
      state: .healthy,
      draining: false,
      busy: false,
      ready: true,
      readinessPhase: .ready,
      readinessStep: .published,
      nativePhase: .live,
      nativeMount: proof,
      brokerPhase: .live
    )
    let encoded = try JSONEncoder().encode(health)
    let decoded = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: encoded)
    #expect(decoded.runtimeBuild == "product-1.8.0")
    #expect(decoded.runtimeProtocol == MountProtocol.runtimeProtocolVersion)
    #expect(decoded.runtimePID == 4242)
    #expect(decoded.processGeneration == "process-7")
    #expect(decoded.activationGeneration == "activation-7")
    #expect(decoded.state == .healthy)
    #expect(decoded.draining == false)
    #expect(decoded.busy == false)
    #expect(decoded.ready)
    #expect(decoded.readinessPhase == .ready)
    #expect(decoded.readinessStep == .published)
    #expect(decoded.nativeMount == proof)
    #expect(decoded.brokerPhase == .live)
  }

  @Test
  func runtimeHealthKeepsLifecycleVerdictOrthogonalAndReportsExactDrain() throws {
    let proof = try MountNativeMountProof(
      presentationRoot: "/Volumes/FuseKit",
      filesystem: MountProtocol.nativeMountFilesystem,
      source: "fuse-t:/FuseKit",
      rootReadEpoch: 7
    )
    let degraded = try MountRuntimeHealthResponse(
      code: .ok,
      message: "",
      runtimeBuild: "product-1.8.0",
      runtimeProtocol: MountProtocol.runtimeProtocolVersion,
      runtimePID: 4242,
      processGeneration: "process-7",
      activationGeneration: "activation-7",
      state: .degraded,
      draining: false,
      busy: true,
      ready: true,
      readinessPhase: .ready,
      readinessStep: .published,
      nativePhase: .live,
      nativeMount: proof,
      brokerPhase: .live
    )
    let decoded = try JSONDecoder().decode(
      MountRuntimeHealthResponse.self,
      from: JSONEncoder().encode(degraded)
    )
    #expect(decoded.state == .degraded)
    #expect(decoded.draining == false)
    #expect(decoded.busy)
    #expect(decoded.ready)
    #expect(decoded.readinessPhase == .ready)
    #expect(decoded.readinessStep == .published)

    let draining = try MountRuntimeHealthResponse(
      code: .ok,
      message: "",
      runtimeBuild: "product-1.8.0",
      runtimeProtocol: MountProtocol.runtimeProtocolVersion,
      runtimePID: 4242,
      processGeneration: "process-7",
      activationGeneration: "activation-7",
      state: .draining,
      draining: true,
      busy: false,
      ready: false,
      readinessPhase: .draining,
      readinessStep: .published,
      nativePhase: .live,
      nativeMount: proof,
      brokerPhase: .live
    )
    let drainingDecoded = try JSONDecoder().decode(
      MountRuntimeHealthResponse.self,
      from: JSONEncoder().encode(draining)
    )
    #expect(drainingDecoded.state == .draining)
    #expect(drainingDecoded.draining)
    #expect(drainingDecoded.ready == false)
    #expect(drainingDecoded.readinessPhase == .draining)
    #expect(drainingDecoded.readinessStep == .published)

    let failed = try MountRuntimeHealthResponse(
      code: .ok,
      message: "",
      runtimeBuild: "product-1.8.0",
      runtimeProtocol: MountProtocol.runtimeProtocolVersion,
      runtimePID: 4242,
      processGeneration: "process-7",
      activationGeneration: "activation-7",
      state: .failed,
      draining: false,
      busy: false,
      ready: false,
      readinessPhase: .failed,
      readinessStep: .published,
      nativePhase: .live,
      nativeMount: proof,
      brokerPhase: .live
    )
    let failedDecoded = try JSONDecoder().decode(
      MountRuntimeHealthResponse.self,
      from: JSONEncoder().encode(failed)
    )
    #expect(failedDecoded.state == .failed)
    #expect(failedDecoded.readinessPhase == .failed)
    #expect(failedDecoded.readinessStep == .published)
  }

  @Test
  func runtimeHealthRejectsInexactReadyAndUnknownFields() throws {
    let missingProof = Data(
      #"{"protocol":1,"code":"ok","message":"","runtime_build":"product-1.8.0","runtime_protocol":1,"runtime_pid":4242,"process_generation":"process-7","activation_generation":"activation-7","state":"healthy","draining":false,"busy":false,"ready":true,"readiness_phase":"ready","readiness_step":"published","native_phase":"live","broker_phase":"disabled"}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.self) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: missingProof)
    }

    let unknown = Data(
      #"{"protocol":1,"code":"ok","message":"","runtime_build":"product-1.8.0","runtime_protocol":1,"runtime_pid":4242,"process_generation":"process-7","activation_generation":"activation-7","state":"degraded","draining":false,"busy":true,"ready":false,"readiness_phase":"starting","readiness_step":"broker","native_phase":"starting","broker_phase":"starting","lifecycle_ready":true}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.self) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: unknown)
    }
  }

  @Test
  func runtimeHealthRejectsMountProofOutsideLiveNativePhase() throws {
    let proof = try MountNativeMountProof(
      presentationRoot: "/Volumes/FuseKit",
      filesystem: MountProtocol.nativeMountFilesystem,
      source: "fuse-t:/FuseKit",
      rootReadEpoch: 7
    )
    #expect(throws: MountProtocolCodingError.self) {
      _ = try MountRuntimeHealthResponse(
        code: .ok,
        message: "",
        runtimeBuild: "product-1.8.0",
        runtimeProtocol: MountProtocol.runtimeProtocolVersion,
        runtimePID: 4242,
        processGeneration: "process-7",
        activationGeneration: "activation-7",
        state: .degraded,
        draining: false,
        busy: false,
        ready: false,
        readinessPhase: .starting,
        readinessStep: .native,
        nativePhase: .starting,
        nativeMount: proof,
        brokerPhase: .disabled
      )
    }
  }

  @Test
  func runtimeHealthPreservesExactRemoteFailure() throws {
    let failure = Data(
      #"{"protocol":1,"code":"unauthorized","message":"peer rejected","runtime_build":"","runtime_protocol":0,"runtime_pid":0,"process_generation":"","activation_generation":"","state":"","draining":false,"busy":false,"ready":false,"readiness_phase":"","readiness_step":"","native_phase":"","broker_phase":""}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.remoteResponse(.unauthorized, "peer rejected")) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: failure)
    }
  }
}
