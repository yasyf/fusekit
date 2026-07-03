# shellcheck shell=bash
# scripts/vm/scenarios/validate-mux.sh — the release gate for single-mount
# multiplexing: ONE native fuse-t mount (one go-nfsv4) serving N source-mode
# tenants as logical subtrees (MountSpec.MuxRoot / the mountd mux_root wire field).
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). It stands up one mux root with >= 3 tenants over a real content
# bridge, drives the claude-shaped xattr/rename/mmap churn against ALL tenants at
# once (the only known nfs_vinvalbuf2 reproducer shape, now consolidated onto one
# native mount), and asserts the mux-specific invariants the feature exists to
# hold. All mount and force-unmount activity is guest-only; the guest workload
# (mux-workload.sh) guards itself against bare metal (exit 86).
#
# Assertions (each fails loud, with per-assertion evidence, in scenario.log):
#   a. exactly one native mount at the mux root + exactly one go-nfsv4 while all
#      tenants are attached, and no subtree carries its own kernel mount;
#   b. per-tenant isolation: each tenant's synth serves its OWN bytes and its
#      carve-out symlink resolves per tenant;
#   c. fileid identity + re-attach coherence: detach/re-attach one tenant
#      repeatedly under load — no fileid aliases two objects, the quiescent tenant's
#      fileid holds across each detach, and each re-attach surfaces the victim's NEW
#      authoritative content (go-nfsv4 is path-keyed, so the victim reclaims its old
#      fileid — content coherence, not fileid freshness, is what re-attach must
#      guarantee);
#   d. detach-under-load: tenant B detaches while tenant A holds an open file and
#      mmap — A is unaffected, B's paths go ENOENT, B then re-serves;
#   e. reassembly: force-unmount the native root (pool quiesced first, mirroring
#      the production idle gate), then re-issue every Mount RPC — the root
#      remounts once and all tenants re-attach and serve;
#   f. no kernel panic for the whole window (the harness's uptime/.panic watcher).
#
# Verdicts: a panic → exit 2 (a mux invariant let the panic surface reach the
# kernel); any assertion or churn failure → exit 1, loud in scenario.log; a full
# clean window with every assertion holding → exit 0.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/mux-workload.sh"
CHURN_CHUNK_S=60
# Window reserved to land the final assertions and a graceful stop before vmctl's
# deadline kill.
STOP_MARGIN_S=180

# guest_workload runs one mux-workload.sh command in the guest; its output lands
# in scenario.log.
guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

log "pushing mux-workload.sh into the guest"
vm_scp_to "$SCENARIO_DIR/mux-workload.sh" "$WORKLOAD"

vm_phase setup
log "setup: mux-serve (one native mount, N source-mode tenants) + per-tenant mmap readers"
guest_workload mux-setup

vm_phase steady-state
log "assertion a+b: one native mount + one go-nfsv4; per-tenant synth/carve-out isolation"
guest_workload mux-onemount
guest_workload mux-isolation

vm_phase fileid-discipline
log "assertion c: detach/re-attach one tenant under load — fileid identity held (no cross-object alias), re-attach content coherent, siblings stable"
guest_workload mux-fileids 6

vm_phase detach-under-load
log "assertion d: detach tenant B while tenant A holds an open file + mmap"
guest_workload mux-detach-load 20
guest_workload mux-onemount

vm_phase reassembly
log "assertion e: native force-unmount (pool quiesced), then re-issue Mount RPCs"
guest_workload mux-forceunmount
guest_workload mux-reassemble
guest_workload mux-onemount
guest_workload mux-isolation

vm_phase churn-fill
log "sustained claude-shaped xattr churn across ALL tenants for the rest of the window"
while (($(vm_seconds_left) > STOP_MARGIN_S)); do
  left="$(vm_seconds_left)"
  chunk=$((left - STOP_MARGIN_S))
  if ((chunk > CHURN_CHUNK_S)); then
    chunk=$CHURN_CHUNK_S
  fi
  ((chunk > 0)) || break
  # Churn all tenants at once; between bursts re-assert one-mount and isolation so
  # a mount, go-nfsv4, or fileid regression under sustained load fails loud.
  guest_workload mux-churn "$chunk" &
  churn_pid=$!
  sleep 5
  guest_workload mux-onemount
  wait "$churn_pid"
  guest_workload mux-isolation
done

vm_phase final
log "final assertions"
guest_workload mux-isolation
guest_workload mux-onemount
guest_workload mux-status || true
guest_workload mux-stop
log "clean window complete"
