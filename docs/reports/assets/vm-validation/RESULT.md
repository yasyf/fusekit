# Mitigation validation — PASS

- Build: `mitigated-3ae83b8` (fusekit main with the nfs_vinvalbuf2 mitigations)
- Scenario: `validate-mitigation.sh` (EXPECT=clean), organic phase-1 churn
- Duration: 72+ min continuous (stopped early — decisive; the unmitigated
  build panics in ~2 s, and a 10 min window is the standing bar going forward)
- Result: **no kernel panic**; guest boottime unchanged the entire run
- Churn: 76 rounds, 45,786 atomic tmp+rename write-through saves,
  ~3,200 parallel reads/round, provenance-style xattr traffic throughout
- AppleDouble gate: 78/78 passes — `._` blocked (EACCES/ENOENT), backing
  litter invisible, ordinary creates/writes/reads unaffected
- Contrast: same workload on unmitigated v0.22.1 → kernel panic 28/28 in ~2 s
