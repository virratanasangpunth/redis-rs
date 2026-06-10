#!/usr/bin/env bash
# Watch server-side cluster topology + health while a node is blocked (block.sh).
#
# Source is CLUSTER NODES, not SLOTS/SHARDS, because it is flat (awk-friendly) and
# uniquely shows the PFAIL intermediate state (flag "fail?") before a node escalates
# to FAIL -- exactly the gossip transition you want to watch after block.sh pauses a
# node and silences its cluster-bus gossip. (SLOTS just silently omits a failed
# replica; you never see it go PFAIL first.)
#
# Usage: watch-slots.sh [PORT] [INTERVAL_SECONDS]
#   PORT      a KNOWN-HEALTHY node to query (NEVER the blocked one). default 7001
#   INTERVAL  seconds between snapshots.                              default 1
#
# Tip: pipe to `tee slots.log` to keep a timestamped history for post-hoc analysis.
set -uo pipefail

PORT=${1:-7001}
INTERVAL=${2:-1}
CONTAINER="cluster-stall-valkey-$((PORT - 7000))"

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then USE_COLOR=1; else USE_COLOR=0; fi

render() {
  awk -v use_color="$USE_COLOR" '
  function col(s, c,   code) {
    if (!use_color) return s
    code = (c=="red")?"31":(c=="yellow")?"33":(c=="green")?"32":(c=="dim")?"2":""
    return (code=="") ? s : "\033[" code "m" s "\033[0m"
  }
  {
    n++
    split($2, a, /[:@]/); p = a[2] + 0
    fl = $3; mid = $4; lk = $8
    s = ""; start = 99999
    for (i = 9; i <= NF; i++) {
      t = $i
      if (substr(t, 1, 1) == "[") continue          # skip migrating/importing markers
      s = s (s == "" ? "" : ",") t
      split(t, r, "-"); v = r[1] + 0; if (v < start) start = v
    }
    P[n]   = p
    LAB[n] = (p >= 7001 && p <= 7006) ? "valkey-" (p - 7000) ":" p : a[1] ":" p
    ROLE[n] = (fl ~ /master/) ? "master" : "replica"
    if      (fl ~ /fail\?/)          H[n] = "PFAIL"
    else if (fl ~ /(^|,)fail(,|$)/)  H[n] = "FAIL"
    else if (lk == "disconnected")   H[n] = "DISC"
    else                             H[n] = "ok"
    LK[n] = lk; SL[n] = (s == "" ? "-" : s); ST[n] = start; MID[n] = mid
    IDLAB[$1] = LAB[n]; IDST[$1] = start
  }
  END {
    if (n == 0) { print "  (no nodes returned)"; exit }
    # sort: by shard (master start slot), masters before their replicas, then port
    for (i = 1; i <= n; i++) {
      SK[i] = (ROLE[i] == "master") ? ST[i] : IDST[MID[i]] + 0
      RO[i] = (ROLE[i] == "master") ? 0 : 1
      o[i]  = i
    }
    for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) {
      x = o[i]; y = o[j]
      if (SK[x] > SK[y] || (SK[x] == SK[y] && (RO[x] > RO[y] || (RO[x] == RO[y] && P[x] > P[y])))) {
        o[i] = y; o[j] = x
      }
    }
    printf "  %-16s %-8s %-7s %-13s %-14s %s\n", "node", "role", "health", "link", "slots", "of-master"
    for (k = 1; k <= n; k++) {
      i = o[k]
      hc = (H[i] == "FAIL") ? "red" : ((H[i] == "PFAIL" || H[i] == "DISC") ? "yellow" : "green")
      hcell = col(sprintf("%-7s", H[i]), hc)
      lcell = (LK[i] == "disconnected") ? col(sprintf("%-13s", LK[i]), "yellow") : sprintf("%-13s", LK[i])
      rcell = (ROLE[i] == "replica") ? col(sprintf("%-8s", ROLE[i]), "dim") : sprintf("%-8s", ROLE[i])
      of = (ROLE[i] == "master") ? "-" : IDLAB[MID[i]]
      printf "  %-16s %s %s %s %-14s %s\n", LAB[i], rcell, hcell, lcell, SL[i], of
    }
  }'
}

echo "watching topology via ${CONTAINER} (port ${PORT}) every ${INTERVAL}s -- Ctrl-C to stop."
echo "(this should be a HEALTHY node; if it shows FAIL/DISC you are likely querying the blocked one)"
while true; do
  ts=$(date '+%H:%M:%S')
  out=$(docker exec "$CONTAINER" valkey-cli -p "$PORT" cluster nodes 2>&1)
  if [[ $? -ne 0 ]]; then
    echo "===== ${ts}  query FAILED via ${CONTAINER} ====="
    echo "  ${out}"
  else
    echo "===== ${ts} ====="
    printf '%s\n' "$out" | render
  fi
  sleep "$INTERVAL"
done
