// Package fileproviderd is the File-Provider analog of mountd: the generic
// control-and-content subsystem a consumer uses to host a macOS File Provider
// domain inside a signed companion app, driven from a plain Go process over two
// 0600 unix sockets.
//
// A plain Go binary cannot host a File Provider domain
// (NSFileProviderReplicatedExtension): the File Provider entitlement lives in a
// code signature, not a per-process grant. So the signed companion app SERVES
// the control protocol and the Go side is the CLIENT (AppClient) — the inverse
// of mountd, where the detached holder is the server. RemoteDomainHost (the
// analog of mountd.RemoteHost) composes AppSpawn with AppClient.
//
// The content half is a framework, not a fixed payload: the signed extension
// reads bulk files from the backing tree directly, but computed items must come
// from the single Go source of truth. BridgeServer binds the data socket and
// dispatches to a consumer-injected ContentSource — fusekit owns the wire and
// the dispatch; the consumer supplies the bytes. BridgeClient is the Go client
// of that socket, for tests and a doctor round-trip.
//
// This package holds ZERO consumer specifics: no merge schema, no Keychain, no
// app-specific imports. It depends only on the standard library, proc, and
// state — the same discipline as mountd.
//
// Compatibility policy (mirrors mountd, NOT its mount-shaped bytes): the proto-1
// wire is FROZEN — op names, request/response field names, and error-class
// strings never change; new capability means a new op or optional field, never a
// rename, repurpose, or retype. The control and data wires version independently
// (ControlProtoVersion, BridgeProtoVersion) because the signed app (control) and
// the sandboxed extension (data) skew on different cadences. An unknown error
// class is treated as the transient ErrAppUnavailable, never a domain failure, so
// additive evolution can never trigger the one irreversible action — retreating
// an account to the symlink floor, which only ClassNoEntitlement does.
package fileproviderd

import (
	"errors"
	"fmt"
)

// ControlProtoVersion stamps every control request and response; the frozen wire keeps it 1.
const ControlProtoVersion = 1

// Op is a control-protocol request operation: the signed companion app serves these, the Go AppClient sends them.
type Op string

const (
	// OpHealth is a liveness + version probe of the companion app.
	OpHealth Op = "health"
	// OpProbe registers a throwaway domain, enumerates it to report whether File
	// Provider can serve here (the capability gate Select keys adoption on), then
	// tears it down before replying.
	OpProbe Op = "probe"
	// OpRegister registers the request's Domain (NSFileProviderManager.add),
	// idempotent if already registered; the reply carries Path, the user-visible
	// domain root.
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

// Request is one control request — a newline-delimited JSON object, one request
// and one response per connection. Domain (the consumer's stable domain id) is
// required by every op except Health and Probe.
type Request struct {
	Proto  int    `json:"proto"`
	Op     Op     `json:"op"`
	Domain string `json:"domain,omitempty"`
}

// Error classes ride alongside Error so clients classify failures without string
// matching. The values are FROZEN; new failure modes get new classes, never a
// rename or repurpose.
const (
	// ClassNoEntitlement: the OS refused the op because the File Provider
	// entitlement is missing or the extension is disabled — a permanent
	// capability "no", not a transient blip.
	ClassNoEntitlement = "no-entitlement"
	// ClassAppUnreachable: the control socket was unreachable or the connection
	// failed mid-op; outcome unknown, clients retry. Transient.
	ClassAppUnreachable = "app-unreachable"
	// ClassRegisterFailed: entitlement present but the OS rejected the domain
	// add/remove for another reason (duplicate id, I/O error). Transient; clients
	// retry.
	ClassRegisterFailed = "register-failed"
	// ClassNoDomain: the op (Path, Signal) named a domain with no registration.
	// Transient — a Register re-creates it.
	ClassNoDomain = "no-domain"
	// ClassBusy: another operation is in flight on the same domain. Transient;
	// retry once it completes.
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
	Path     string `json:"path,omitempty"`    // register, path
}

// Control-protocol error sentinels: AppClient maps Response.ErrClass onto these
// for errors.Is, preserving the app's raw Error string (which carries any
// user-facing guidance, e.g. the enablement walkthrough) in the returned message.
var (
	// ErrCannotControl means the companion app cannot drive File Provider here
	// (entitlement missing or extension disabled) — a permanent capability "no",
	// the ONLY condition that retreats an account to the symlink floor. It must
	// never errors.Is-match ErrAppUnavailable, or a transient blip would trigger
	// that irreversible retreat.
	ErrCannotControl = errors.New("companion app cannot control File Provider (no entitlement or extension disabled)")
	// ErrAppUnavailable means the companion app was unreachable or a control
	// connection failed mid-op — a process-availability condition (aliased to
	// proc.ErrChildUnavailable in appclient.go), never a domain verdict, so
	// clients retry.
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
// (forward skew: newer app, older client) maps to ErrAppUnavailable, never
// ErrCannotControl, so additive evolution can never trigger the irreversible
// retreat.
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

// errToClass is the inverse of classToErr, for the companion app side; an error
// matching no sentinel yields "" so the app sends the bare message.
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

// respErr converts a failed control response into an error: a bare error when
// the app sent no class, else the class sentinel wrapped around the app's raw
// message.
func respErr(resp *Response) error {
	if resp.OK {
		return nil
	}
	if resp.ErrClass == "" {
		return errors.New(resp.Error)
	}
	return fmt.Errorf("%w: %s", classToErr(resp.ErrClass), resp.Error)
}
