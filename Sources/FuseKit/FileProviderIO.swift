import Foundation

actor CatalogBindingGate {
  private let binding: CatalogFileProviderBinding
  private let client: CatalogClient
  private var task: Task<Void, Error>?
  private var bound = false

  init(binding: CatalogFileProviderBinding, client: CatalogClient) {
    self.binding = binding
    self.client = client
  }

  func bind() async throws {
    if bound {
      return
    }
    if let task {
      return try await task.value
    }
    let task = Task {
      try await client.bind(domainID: binding.domainID, tenant: binding.tenant)
    }
    self.task = task
    do {
      try await task.value
      bound = true
      self.task = nil
    } catch {
      self.task = nil
      throw error
    }
  }
}

actor CatalogFileUploadCursor {
  private let handle: FileHandle
  private var finished = false

  init(url: URL) throws {
    handle = try FileHandle(forReadingFrom: url)
  }

  func next() throws -> Data? {
    guard !finished else { return nil }
    do {
      guard let data = try handle.read(upToCount: 1024 * 1024), !data.isEmpty else {
        finished = true
        try handle.close()
        return nil
      }
      return data
    } catch {
      finished = true
      try? handle.close()
      throw error
    }
  }

  func cancel() {
    guard !finished else { return }
    finished = true
    try? handle.close()
  }
}
