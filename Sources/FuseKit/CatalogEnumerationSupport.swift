@preconcurrency import FileProvider
import Foundation

enum CatalogEnumerationTokenScopeKind: String, Codable {
  case container
  case workingSet
}

struct CatalogEnumerationTokenContext: Codable, Equatable {
  let domainID: CatalogDomainID
  let tenantID: CatalogTenantID
  let generation: UInt64
  let rootID: CatalogObjectID
  let scope: CatalogEnumerationTokenScopeKind
  let parentID: CatalogObjectID?
}

struct CatalogEnumerationPageToken: Codable {
  let version: UInt8
  let context: CatalogEnumerationTokenContext
  let revision: UInt64
  let after: CatalogObjectID?
}

struct CatalogEnumerationChangeAnchor: Codable {
  let version: UInt8
  let context: CatalogEnumerationTokenContext
  let cursor: CatalogChangeCursor
}

struct CatalogChangeEmitter {
  let binding: CatalogFileProviderBinding

  func emit(
    _ changes: [CatalogChange],
    to observer: any NSFileProviderChangeObserver
  ) {
    let deletions = changes.filter { $0.kind == .delete }.map { identifier($0.object.id) }
    let updates = changes.filter { $0.kind == .upsert }.map {
      CatalogFileProviderItem(
        object: $0.object,
        rootID: binding.rootID,
        accessMode: binding.accessMode
      )
    }
    if !deletions.isEmpty {
      observer.didDeleteItems(withIdentifiers: deletions)
    }
    if !updates.isEmpty {
      observer.didUpdate(updates)
    }
  }

  private func identifier(_ objectID: CatalogObjectID) -> NSFileProviderItemIdentifier {
    objectID == binding.rootID ? .rootContainer : NSFileProviderItemIdentifier(objectID.rawValue)
  }
}

enum CatalogEnumerationError {
  static func change(_ error: Error) -> Error {
    if case let CatalogClientError.response(code, _) = error, code == .staleAnchor {
      return NSFileProviderError(.syncAnchorExpired)
    }
    return error
  }

  static func page(_ error: Error) -> Error {
    if case let CatalogClientError.response(code, _) = error, code == .staleAnchor {
      return NSFileProviderError(.pageExpired)
    }
    return error
  }
}
