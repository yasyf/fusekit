// Package fusekit exposes the stable tenant surface of a revisioned filesystem
// runtime. The catalog owns opaque object identity, transactional namespace
// changes, immutable content snapshots, and revision deltas. Tenant actors
// coalesce convergence work, while mount and File Provider present the same
// catalog state.
//
// The holder subpackage validates one consumer-owned signed-application plan
// and composes daemonkit lifecycle, exact persistent transport, disposable
// workers, peer trust, and one signed native mount child. The Swift FuseKit
// product supplies the corresponding File Provider runtime and signed App Group
// broker. Product-specific accounts, credentials, content policy, application
// packaging, bundle identifiers, and entitlements remain in each consumer.
package fusekit
