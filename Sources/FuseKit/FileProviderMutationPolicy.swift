import FileProvider

/// CatalogFileProviderMutationDispositionPolicy classifies each File Provider create explicitly.
public protocol CatalogFileProviderMutationDispositionPolicy: Sendable {
  func disposition(
    for template: any NSFileProviderItem,
    fields: NSFileProviderItemFields
  ) throws -> CatalogMutationDisposition
}
