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
	// OpProbeDomain enumerates the request's Domain and reads its .claude.json
	// WITHOUT a materializing filesystem read (which would trip TCC), reporting
	// whether the domain serves and, via JSONBytes, the .claude.json byte count.
	// The readiness poll Setup runs to gate a cutover keys on it.
	OpProbeDomain Op = "probe-domain"
	// OpListDomains returns every File Provider domain the platform has
	// registered for the app (NSFileProviderManager.getDomains) — orphans
	// included, which is the point: consumers reconcile intended domains
	// against this truth. Additive; an app predating the op answers its
	// unknown-op default arm, which AppClient.ListDomains maps to
	// ErrOpUnsupported.
	OpListDomains Op = "list-domains"
	// OpPrepareDomain force-materializes the domain's computed settings.json
	// (requestDownloadForItem then a bounded wait) so a live session's first read
	// never blocks on a cold File Provider fetch. Additive; an app predating the
	// op answers its unknown-op default arm, which AppClient.PrepareDomain maps to
	// ErrOpUnsupported.
	OpPrepareDomain Op = "prepare-domain"
)

// Request is one control request — a newline-delimited JSON object, one request
// and one response per connection. Domain (the consumer's stable domain id) is
// required by every op except Health and Probe.
type Request struct {
	Proto  int    `json:"proto"`
	Op     Op     `json:"op"`
	Domain string `json:"domain,omitempty"`
	// Shallow selects probe-domain's non-materializing arm: domain lookup +
	// getUserVisibleURL + a readdir of the domain root ONLY, no byte read. Absent
	// (false) means the deep byte-reading probe — an app predating the flag ignores
	// it and answers deep, the designed skew. Additive optional field on the frozen
	// wire.
	Shallow bool `json:"shallow,omitempty"`
	// DeadlineMS bounds prepare-domain's materialization wait in milliseconds; 0
	// (absent) lets the app choose its default (~30s). Additive optional field on
	// the frozen wire.
	DeadlineMS int64 `json:"deadline_ms,omitempty"`
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
	// ClassDomainNotServing: ProbeDomain could not enumerate/read the domain — it
	// registered but has not materialized enough to answer, or the read failed or
	// timed out. Transient; the readiness poll keeps trying until its deadline.
	ClassDomainNotServing = "domain-not-serving"
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
	// JSONBytes carries probe-domain's .claude.json verdict. A POINTER so the
	// serving-but-empty case (0) survives the wire while the serving-but-absent
	// case omits the field entirely: absent (nil) = the domain serves but
	// .claude.json does not exist; 0 = it exists and is empty; >0 = bytes actually
	// READ (never a stat — FPFS lies at size 0).
	JSONBytes *int64 `json:"json_bytes,omitempty"` // probe-domain
	// Listed carries a SHALLOW probe-domain verdict: whether ".claude.json" appears
	// in the readdir of the domain root. A POINTER so it is present ONLY when the app
	// actually answered a shallow probe; a deep reply (and an app too old to know the
	// flag, which answers deep) omits it entirely, and AppClient.ProbeDomainShallow
	// then derives the verdict from the deep JSONBytes shape.
	Listed *bool `json:"listed,omitempty"` // probe-domain (shallow)
	// Domains: list-domains returns the platform's registered domains.
	// Additive; empty in every other reply.
	Domains []DomainInfo `json:"domains,omitempty"`
}

// DomainInfo is one registered File Provider domain in a list-domains reply.
type DomainInfo struct {
	// Domain is the stable domain identifier.
	Domain string `json:"domain"`
	// DisplayName is the user-visible name the domain was registered with.
	DisplayName string `json:"display_name,omitempty"`
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
	// ErrDomainNotServing means a registered domain did not serve an enumeration
	// (ProbeDomain's ClassDomainNotServing), or the readiness poll hit its deadline
	// still waiting for one: NSFileProviderManager.add returned but the appex has not
	// materialized the domain enough to answer a read. Not a permanent verdict —
	// Setup returns it rather than cutting an account dir over to a domain that
	// cannot yet serve reads, the exact pre-readiness cutover that crushed the File
	// Provider host under a fleet migrate.
	ErrDomainNotServing = errors.New("file provider domain did not serve an enumeration in time")
	// ErrOpUnsupported means the companion app is too old to know a control op: it
	// answered its unknown-op default arm (ok:false, EMPTY err_class). ONLY
	// ProbeDomain and ListDomains map this shape to a sentinel (other ops surface
	// the bare unknown-op message); it must never errors.Is-match a transient or
	// retreat sentinel — an old app is a hard, operator-actionable "upgrade the
	// app" condition, not a blip and not a capability retreat.
	ErrOpUnsupported = errors.New("companion app does not support this control op (upgrade the app)")
	// ErrDomainRemovalUnconfirmed means RemoveConfirmed could not confirm the domain
	// left fileproviderd's list within its window (a deferred add can resurrect it as
	// an orphan). Client-side only — never a wire class.
	ErrDomainRemovalUnconfirmed = errors.New("file provider domain removal could not be confirmed; the domain may materialize later as an orphan — reconcile with the consumer's doctor tooling")
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
	case ClassDomainNotServing:
		return ErrDomainNotServing
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
	case errors.Is(err, ErrDomainNotServing):
		return ClassDomainNotServing
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
