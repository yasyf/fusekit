import Foundation
@testable import FuseKit
import Testing

@Suite("Mount protocol")
struct MountProtocolTests {
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
      runtimeBuild: "product-1.7.8",
      activationGeneration: "activation-7",
      readinessPhase: .ready,
      readinessStep: .published,
      nativePhase: .live,
      nativeMount: proof,
      brokerPhase: .live
    )
    let encoded = try JSONEncoder().encode(health)
    let decoded = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: encoded)
    #expect(decoded.runtimeBuild == "product-1.7.8")
    #expect(decoded.activationGeneration == "activation-7")
    #expect(decoded.readinessPhase == .ready)
    #expect(decoded.readinessStep == .published)
    #expect(decoded.nativeMount == proof)
    #expect(decoded.brokerPhase == .live)
  }

  @Test
  func runtimeHealthRejectsInexactReadyAndUnknownFields() throws {
    let missingProof = Data(
      #"{"protocol":1,"code":"ok","message":"","runtime_build":"product-1.7.8","activation_generation":"activation-7","readiness_phase":"ready","readiness_step":"published","native_phase":"live","broker_phase":"disabled"}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.self) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: missingProof)
    }

    let unknown = Data(
      #"{"protocol":1,"code":"ok","message":"","runtime_build":"product-1.7.8","activation_generation":"activation-7","readiness_phase":"starting","readiness_step":"broker","native_phase":"starting","broker_phase":"starting","lifecycle_ready":true}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.self) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: unknown)
    }
  }

  @Test
  func runtimeHealthPreservesExactRemoteFailure() throws {
    let failure = Data(
      #"{"protocol":1,"code":"unauthorized","message":"peer rejected","runtime_build":"","activation_generation":"","readiness_phase":"","readiness_step":"","native_phase":"","broker_phase":""}"#.utf8
    )
    #expect(throws: MountProtocolCodingError.remoteResponse(.unauthorized, "peer rejected")) {
      _ = try JSONDecoder().decode(MountRuntimeHealthResponse.self, from: failure)
    }
  }
}
