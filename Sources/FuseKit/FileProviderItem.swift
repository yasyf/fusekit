import FileProvider
import Foundation
import UniformTypeIdentifiers

/// CatalogFileProviderItem is an immutable exact catalog-object presentation.
public final class CatalogFileProviderItem: NSObject, NSFileProviderItem {
  public let itemIdentifier: NSFileProviderItemIdentifier
  public let parentItemIdentifier: NSFileProviderItemIdentifier
  public let filename: String
  public let contentType: UTType
  public let capabilities: NSFileProviderItemCapabilities
  public let documentSize: NSNumber?
  public let symlinkTargetPath: String?
  public let contentPolicy: NSFileProviderContentPolicy = .inherited
  public let itemVersion: NSFileProviderItemVersion

  public init(
    object: CatalogObject,
    rootID: CatalogObjectID,
    accessMode: CatalogTenantAccessMode
  ) {
    let isRoot = object.id == rootID
    itemIdentifier = isRoot ? .rootContainer : NSFileProviderItemIdentifier(object.id.rawValue)
    parentItemIdentifier =
      isRoot || object.parentID == rootID
        ? .rootContainer : NSFileProviderItemIdentifier(object.parentID.rawValue)
    filename = object.name
    switch object.kind {
    case .directory: contentType = .folder
    case .file: contentType = .data
    case .symlink: contentType = .symbolicLink
    }
    documentSize = object.kind == .file ? NSNumber(value: object.size) : nil
    symlinkTargetPath = object.kind == .symlink ? object.linkTarget : nil
    if isRoot {
      var rootCapabilities: NSFileProviderItemCapabilities = [
        .allowsReading, .allowsContentEnumerating,
      ]
      if accessMode == .readWrite {
        rootCapabilities.insert(.allowsAddingSubItems)
      }
      capabilities = rootCapabilities
    } else {
      var common: NSFileProviderItemCapabilities = [.allowsReading]
      if accessMode == .readWrite {
        common.formUnion([.allowsRenaming, .allowsReparenting, .allowsDeleting])
      }
      switch object.kind {
      case .directory:
        capabilities =
          accessMode == .readWrite
            ? common.union([.allowsContentEnumerating, .allowsAddingSubItems])
            : common.union(.allowsContentEnumerating)
      case .file:
        capabilities = accessMode == .readWrite ? common.union(.allowsWriting) : common
      case .symlink:
        capabilities = common
      }
    }
    itemVersion = NSFileProviderItemVersion(
      contentVersion: Self.versionData(object.contentRevision),
      metadataVersion: Self.versionData(object.metadataRevision)
    )
    super.init()
  }

  private static func versionData(_ revision: UInt64) -> Data {
    var value = revision.bigEndian
    return Data(bytes: &value, count: MemoryLayout<UInt64>.size)
  }
}
