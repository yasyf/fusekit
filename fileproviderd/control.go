// Package fileproviderd is the File-Provider analog of mountd: the generic
// control-and-content subsystem a consumer uses to host a macOS File Provider
// domain inside a signed companion app, driven from a plain Go process over two
// 0600 unix sockets.
//
// A File Provider domain (NSFileProviderReplicatedExtension) cannot be hosted by
// a plain Go binary — NSFileProviderManager.add requires the File Provider
// entitlement, which lives in a code signature, not a per-process grant. So a
// signed companion app SERVES the control protocol (Health/Probe/Register/Path/
// Signal/Remove) and the Go side is the CLIENT (AppClient), the inverse of
// mountd where the detached holder is the server. The lifecycle half a consumer
// overlay provider embeds is RemoteDomainHost (the analog of mountd.RemoteHost):
// it composes AppSpawn (ensure the signed app is running) with AppClient (drive
// the domain ops).
//
// The content half is a framework, not a fixed payload: the signed extension
// reads bulk shared/private files from the backing tree directly, but a handful
// of computed items (a merged config file, an injected settings file) must come
// from the single Go source of truth. BridgeServer binds the data socket and
// dispatches Manifest/ReadSynth/WriteThrough/Classify to a consumer-injected
// ContentSource — fusekit owns the wire and the dispatch; the consumer supplies
// the bytes. BridgeClient is the Go client of that socket, for tests and a
// doctor round-trip.
//
// This package holds ZERO consumer specifics: no merge schema, no Keychain, no
// app-specific imports. It depends only on the standard library, proc, and
// state — the same discipline as mountd.
//
// Compatibility policy (mirrors mountd, NOT its mount-shaped bytes): the proto-1
// wire is FROZEN. Op names, request/response field names, and error-class
// strings never change. New capability means a new op or a new optional field
// with a new name, never a rename, repurpose, or retype. The control and data
// wires version independently (ControlProtoVersion, BridgeProtoVersion) because
// the signed app (control) and the sandboxed extension (data) ship and skew on
// different cadences. A client that receives an unknown error class treats it as
// the transient ErrAppUnavailable, never as a domain failure — so additive
// protocol evolution can never trigger the one irreversible action (retreating
// an account to the symlink floor), which only ClassNoEntitlement does.
package fileproviderd

import (
	"errors"
	"fmt"
)

// ControlProtoVersion stamps every control request and response. It is bumped
// only on an incompatible wire change — which the compatibility policy rules
// out — so in practice it stays 1.
const ControlProtoVersion = 1

// Op is a control-protocol request operation. The signed companion app serves
// these; the Go AppClient sends them.
type Op string

const (
	// OpHealth is a liveness + version probe of the companion app.
	OpHealth Op = "health"
	// OpProbe registers a throwaway domain and enumerates it, reporting whether
	// File Provider can serve on this machine — the capability gate Select keys
	// adoption on. The app tears the probe domain down before replying.
	OpProbe Op = "probe"
	// OpRegister ensures a domain for the request's Domain is registered
	// (NSFileProviderManager.add); idempotent for an already-registered domain.
	// The reply carries Path, the user-visible domain root.
	OpRegister Op = "register"
	// OpPath returns the user-visible domain root for an already-registered
	// domain (getUserVisibleURL) without re-registering.
	OpPath Op = "path"
	// OpSignal tells the app to signal the domain's enumerator
	// (signalEnumerator) so the OS re-enumerates after a backing-tree change.
	OpSignal Op = "signal"
	// OpRemove deregisters the domain (NSFileProviderManager.remove). A domain
	// that is not registered is an OK no-op.
	OpRemove Op = "remove"
)

// Request is one control request: one JSON object, newline-delimited, one
// request and one response per connection. Domain is the consumer's stable
// domain identifier (e.g. an account id); it is required by every op except
// Health and Probe.
type Request struct {
	Proto  int    `json:"proto"`
	Op     Op     `json:"op"`
	Domain string `json:"domain,omitempty"`
}

// Error classes ride alongside Error so clients classify failures without
// string matching. The values are FROZEN; new failure modes get new classes,
// and a client that predates a class treats it as ClassNoEntitlement's
// opposite — the transient ErrAppUnavailable — never as a domain failure.
const (
	// ClassNoEntitlement: the companion app reached NSFileProviderManager but
	// the OS refused the operation because the File Provider entitlement is
	// missing or the extension is disabled in System Settings. This is the ONLY
	// class that should retreat an account to the symlink floor — it is a
	// permanent capability "no" on this machine, not a transient blip. The Error
	// text carries the user-facing enablement walkthrough.
	ClassNoEntitlement = "no-entitlement"
	// ClassAppUnreachable: the control socket could not be reached, or an
	// established connection failed mid-op (the app died, an EOF). The op's
	// outcome is unknown; clients retry. Transient.
	ClassAppUnreachable = "app-unreachable"
	// ClassRegisterFailed: NSFileProviderManager.add/remove was reached and the
	// entitlement is present, but the OS rejected the domain operation for some
	// other reason (a duplicate identifier the app could not reconcile, an I/O
	// error materializing the domain root). Transient; clients retry.
	ClassRegisterFailed = "register-failed"
	// ClassNoDomain: the op named a domain the app has no registration for, and
	// the op requires one (Path, Signal). Transient from the client's view —
	// a Register re-creates it.
	ClassNoDomain = "no-domain"
	// ClassBusy: another operation is in flight on the same domain. Transient;
	// the caller may retry once it completes.
	ClassBusy = "busy"
)

// Response is one control reply: one JSON object per line.
type Response struct {
	Proto    int    `json:"proto"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	ErrClass string `json:"err_class,omitempty"`
	Version  string `json:"version,omitempty"` // health
	FPOK     bool   `json:"fp_ok,omitempty"`   // probe
	Path     string `json:"path,omitempty"`    // register, path: the user-visible domain root
}

// Control-protocol error sentinels. AppClient maps Response.ErrClass onto these
// so callers classify with errors.Is; the app's raw Error string — which
// carries any user-facing guidance, e.g. the enablement walkthrough — is
// preserved in the returned error's message.
var (
	// ErrCannotControl means the companion app cannot drive File Provider on
	// this machine: the entitlement is missing or the extension is disabled. It
	// is the File-Provider analog of mountd.ErrCannotHost — a PERMANENT
	// capability "no", the ONLY condition that retreats an account to the
	// symlink floor. It must NEVER errors.Is-match ErrAppUnavailable: collapsing
	// the two would let a transient app blip trigger the one irreversible
	// retreat.
	ErrCannotControl = errors.New("companion app cannot control File Provider (no entitlement or extension disabled)")
	// ErrAppUnavailable means the companion app could not be reached or an
	// established control connection failed mid-op. It is the control-domain
	// alias of proc.ErrChildUnavailable (set in appclient.go) — a process-
	// availability condition, never a domain verdict, so clients retry. Every
	// unrecognized error class maps here too: additive protocol evolution must
	// fail toward retry, never toward the ErrCannotControl retreat.
	ErrAppUnavailable error
	// ErrRegisterFailed means a reached, entitled app's domain register/remove
	// was rejected by the OS for a non-entitlement reason. Transient.
	ErrRegisterFailed = errors.New("File Provider domain registration failed")
	// ErrNoDomain means the op named a domain the app has no registration for.
	// Transient; a Register re-creates it.
	ErrNoDomain = errors.New("no such registered domain")
	// ErrBusy means another operation is in flight on the same domain.
	// Transient; safe to retry once it completes.
	ErrBusy = errors.New("domain busy with another operation")
)

// classToErr maps a wire error class onto its sentinel. An unrecognized class
// (forward skew: a newer app behind an older client) maps to ErrAppUnavailable,
// the transient retry path — never to ErrCannotControl, so additive evolution
// can never trigger the irreversible retreat. An empty class means the app sent
// a bare message, returned verbatim by respErr.
func classToErr(class string) error {
	switch class {
	case ClassNoEntitlement:
		return ErrCannotControl
	case ClassAppUnreachable:
		return ErrAppUnavailable
	case ClassRegisterFailed:
		return ErrRegisterFailed
	case ClassNoDomain:
		return ErrNoDomain
	case ClassBusy:
		return ErrBusy
	default:
		return ErrAppUnavailable
	}
}

// errToClass maps a sentinel onto its wire error class, for the companion app
// side (and any Go fake standing in for it). It is the inverse of classToErr
// over the known sentinels. An error that matches no sentinel yields "" — the
// app sends the bare message.
func errToClass(err error) string {
	switch {
	case errors.Is(err, ErrCannotControl):
		return ClassNoEntitlement
	case errors.Is(err, ErrAppUnavailable):
		return ClassAppUnreachable
	case errors.Is(err, ErrRegisterFailed):
		return ClassRegisterFailed
	case errors.Is(err, ErrNoDomain):
		return ClassNoDomain
	case errors.Is(err, ErrBusy):
		return ClassBusy
	default:
		return ""
	}
}

// respErr converts a failed control response into an error: the matching class
// sentinel wrapped around the app's raw message, ErrAppUnavailable for a class
// this client predates (forward skew — retryable, never a retreat), or a plain
// error when the app sent no class at all.
func respErr(resp *Response) error {
	if resp.OK {
		return nil
	}
	if resp.ErrClass == "" {
		return errors.New(resp.Error)
	}
	return fmt.Errorf("%w: %s", classToErr(resp.ErrClass), resp.Error)
}
