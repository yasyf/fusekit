package mountd

import (
	"fmt"
	"path/filepath"
	"time"
)

// MaxAttestTTL caps an OpAttestIdle attestation's freshness: a consumer must
// keep re-attesting while idle, so a crashed consumer's stale attestation can
// never let the holder drain a mount that came back into use.
const MaxAttestTTL = 15 * time.Minute

// attestation is one consumer's recorded idleness claim for a dir.
type attestation struct {
	owner   string
	expires time.Time
}

// handleAttestIdle records Request.Owner's idleness attestation for each of
// Request.Dirs, fresh until now+TTL. A dir registered to a DIFFERENT owner is
// refused (one consumer must never get another's mount drained); an
// unregistered dir is recorded — needed while a retire sweep holds a
// journaled mount deregistered — and any later (re)registration clears it
// (setupAndRegister), so a pre-mount attestation never gates a mount created
// after it.
func (s *Server) handleAttestIdle(req Request) Response {
	if !validOwner(req.Owner) {
		return Response{OK: false, ErrClass: ClassInvalidOwner, Error: fmt.Sprintf("attestidle: owner %q must be a safe single path segment", req.Owner)}
	}
	if len(req.Dirs) == 0 {
		return Response{OK: false, Error: "attestidle: dirs are required"}
	}
	if req.TTL <= 0 || req.TTL > MaxAttestTTL {
		return Response{OK: false, Error: fmt.Sprintf("attestidle: ttl must be in (0, %s], got %s", MaxAttestTTL, req.TTL)}
	}
	for _, dir := range req.Dirs {
		if !filepath.IsAbs(dir) {
			return Response{OK: false, Error: fmt.Sprintf("attestidle: dir %q must be absolute", dir)}
		}
		if row, ok := s.registered(dir); ok && row.Owner != req.Owner {
			return Response{OK: false, ErrClass: ClassForeignMount, Error: fmt.Sprintf("attestidle: %s is owned by %q, not %q", dir, row.Owner, req.Owner)}
		}
	}
	now := time.Now()
	expires := now.Add(req.TTL)
	s.attestMu.Lock()
	for dir, a := range s.attests {
		if now.After(a.expires) {
			delete(s.attests, dir)
		}
	}
	for _, dir := range req.Dirs {
		s.attests[dir] = attestation{owner: req.Owner, expires: expires}
	}
	s.attestMu.Unlock()
	return Response{OK: true}
}

// handleRevokeIdle synchronously withdraws Request.Owner's OWN recorded
// attestations for Request.Dirs, so a retire that has not yet swept can no
// longer treat them as idle — the consumer's select path revokes before it
// hands a mount to a new session. Only the owner's own attestations are
// removed; a dir attested by a different owner, or not at all, is an
// idempotent no-op — a revoke only ever narrows what a retire may drain, so
// there is nothing to refuse.
func (s *Server) handleRevokeIdle(req Request) Response {
	if !validOwner(req.Owner) {
		return Response{OK: false, ErrClass: ClassInvalidOwner, Error: fmt.Sprintf("revokeidle: owner %q must be a safe single path segment", req.Owner)}
	}
	if len(req.Dirs) == 0 {
		return Response{OK: false, Error: "revokeidle: dirs are required"}
	}
	for _, dir := range req.Dirs {
		if !filepath.IsAbs(dir) {
			return Response{OK: false, Error: fmt.Sprintf("revokeidle: dir %q must be absolute", dir)}
		}
	}
	s.attestMu.Lock()
	for _, dir := range req.Dirs {
		if a, ok := s.attests[dir]; ok && a.owner == req.Owner {
			delete(s.attests, dir)
		}
	}
	s.attestMu.Unlock()
	return Response{OK: true}
}

// clearAttest drops dir's recorded attestation: every mount (re)registration
// invalidates a pre-mount idleness verdict — the consumer must attest against
// the mount that now exists before a retire may drain it.
func (s *Server) clearAttest(dir string) {
	s.attestMu.Lock()
	delete(s.attests, dir)
	s.attestMu.Unlock()
}

// attestFresh reports whether dir holds an unexpired attestation from owner.
// Fail-closed on every mismatch: no attestation, a different owner's, or an
// expired one all read not-idle. An ownerless (legacy) mount can never match
// (validOwner refuses ""), so it defers a self-retire indefinitely unless its
// spec opts into IdlePolicy "probe".
func (s *Server) attestFresh(dir, owner string, now time.Time) bool {
	s.attestMu.Lock()
	defer s.attestMu.Unlock()
	a, ok := s.attests[dir]
	return ok && a.owner == owner && now.Before(a.expires)
}
