@preconcurrency import FileProvider
import Foundation

/// CatalogFileProviderConfigurationError reports a missing consumer policy.
public enum CatalogFileProviderConfigurationError: Error, Sendable {
  case runtimeNotConfigured
}

/// CatalogFileProviderOperationError rejects File Provider options outside the catalog contract.
public enum CatalogFileProviderOperationError: Error, Equatable, Sendable {
  case unsupportedCreateOptions
  case unsupportedModifyOptions
  case unsupportedDeleteOptions
}

enum CatalogFileProviderOperationPolicy {
  static let appliedFields: NSFileProviderItemFields = [
    .filename, .parentItemIdentifier, .contents,
  ]

  static func validate(_ options: NSFileProviderCreateItemOptions) throws {
    guard options.isEmpty else {
      throw CatalogFileProviderOperationError.unsupportedCreateOptions
    }
  }

  static func validate(_ options: NSFileProviderModifyItemOptions) throws {
    guard options.isEmpty else {
      throw CatalogFileProviderOperationError.unsupportedModifyOptions
    }
  }

  static func validate(_ options: NSFileProviderDeleteItemOptions) throws {
    guard options.isEmpty else {
      throw CatalogFileProviderOperationError.unsupportedDeleteOptions
    }
  }

  static func remaining(_ fields: NSFileProviderItemFields) -> NSFileProviderItemFields {
    fields.subtracting(appliedFields)
  }
}

/// CatalogReplicatedExtension is the complete generic extension surface.
/// Consumers subclass it and override only ``makeRuntime(for:binding:)``.
open class CatalogReplicatedExtension: NSObject, NSFileProviderReplicatedExtension,
  @unchecked Sendable
{
  public let domain: NSFileProviderDomain
  public let runtime: CatalogFileProviderRuntime

  open class func makeRuntime(
    for _: NSFileProviderDomain,
    binding _: CatalogFileProviderBinding
  ) throws -> CatalogFileProviderRuntime {
    throw CatalogFileProviderConfigurationError.runtimeNotConfigured
  }

  public required init(domain: NSFileProviderDomain) {
    self.domain = domain
    do {
      let binding = try CatalogFileProviderBinding(domain: domain)
      runtime = try Self.makeRuntime(for: domain, binding: binding)
    } catch {
      preconditionFailure("FuseKit File Provider runtime configuration failed: \(error)")
    }
    super.init()
  }

  open func invalidate() {
    runtime.invalidate()
  }

  open func materializedItemsDidChange(completionHandler: @escaping () -> Void) {
    runtime.materializedItemsDidChange()
    completionHandler()
  }

  open func item(
    for identifier: NSFileProviderItemIdentifier,
    request _: NSFileProviderRequest,
    completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void
  ) -> Progress {
    perform(completionHandler: completionHandler) {
      try await self.runtime.item(for: identifier)
    }
  }

  open func fetchContents(
    for itemIdentifier: NSFileProviderItemIdentifier,
    version requestedVersion: NSFileProviderItemVersion?,
    request _: NSFileProviderRequest,
    completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    let version = Unchecked(requestedVersion)
    let completion = Unchecked(completionHandler)
    let task = Task {
      do {
        let (url, item) = try await runtime.fetchContents(
          for: itemIdentifier,
          requestedVersion: version.value
        )
        completion.value(url, item, nil)
      } catch {
        completion.value(nil, nil, error)
      }
      progress.completedUnitCount = 1
    }
    progress.cancellationHandler = { task.cancel() }
    return progress
  }

  open func createItem(
    basedOn itemTemplate: NSFileProviderItem,
    fields: NSFileProviderItemFields,
    contents url: URL?,
    options: NSFileProviderCreateItemOptions,
    request _: NSFileProviderRequest,
    completionHandler:
      @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    let template = Unchecked(itemTemplate)
    let completion = Unchecked(completionHandler)
    let task = Task {
      do {
        try CatalogFileProviderOperationPolicy.validate(options)
        let item = try await runtime.create(
          template: template.value,
          fields: fields,
          contents: url
        )
        completion.value(item, CatalogFileProviderOperationPolicy.remaining(fields), false, nil)
      } catch {
        completion.value(nil, fields, false, error)
      }
      progress.completedUnitCount = 1
    }
    progress.cancellationHandler = { task.cancel() }
    return progress
  }

  open func modifyItem(
    _ item: NSFileProviderItem,
    baseVersion: NSFileProviderItemVersion,
    changedFields: NSFileProviderItemFields,
    contents newContents: URL?,
    options: NSFileProviderModifyItemOptions,
    request _: NSFileProviderRequest,
    completionHandler:
      @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    let proposedItem = Unchecked(item)
    let version = Unchecked(baseVersion)
    let completion = Unchecked(completionHandler)
    let task = Task {
      do {
        try CatalogFileProviderOperationPolicy.validate(options)
        let result = try await runtime.modify(
          item: proposedItem.value,
          baseVersion: version.value,
          changedFields: changedFields,
          contents: newContents
        )
        completion.value(
          result,
          CatalogFileProviderOperationPolicy.remaining(changedFields),
          false,
          nil
        )
      } catch {
        completion.value(nil, changedFields, false, error)
      }
      progress.completedUnitCount = 1
    }
    progress.cancellationHandler = { task.cancel() }
    return progress
  }

  open func deleteItem(
    identifier: NSFileProviderItemIdentifier,
    baseVersion: NSFileProviderItemVersion,
    options: NSFileProviderDeleteItemOptions,
    request _: NSFileProviderRequest,
    completionHandler: @escaping (Error?) -> Void
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    let version = Unchecked(baseVersion)
    let completion = Unchecked(completionHandler)
    let task = Task {
      do {
        try CatalogFileProviderOperationPolicy.validate(options)
        try await runtime.delete(identifier: identifier, baseVersion: version.value)
        completion.value(nil)
      } catch {
        completion.value(error)
      }
      progress.completedUnitCount = 1
    }
    progress.cancellationHandler = { task.cancel() }
    return progress
  }

  open func enumerator(
    for containerItemIdentifier: NSFileProviderItemIdentifier,
    request _: NSFileProviderRequest
  ) throws -> NSFileProviderEnumerator {
    try runtime.enumerator(for: containerItemIdentifier)
  }

  private func perform(
    completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void,
    operation: @escaping @Sendable () async throws -> NSFileProviderItem
  ) -> Progress {
    let progress = Progress(totalUnitCount: 1)
    let completion = Unchecked(completionHandler)
    let task = Task {
      do {
        try await completion.value(operation(), nil)
      } catch {
        completion.value(nil, error)
      }
      progress.completedUnitCount = 1
    }
    progress.cancellationHandler = { task.cancel() }
    return progress
  }
}

private final class Unchecked<Value>: @unchecked Sendable {
  let value: Value

  init(_ value: Value) {
    self.value = value
  }
}
