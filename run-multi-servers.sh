#!/usr/bin/env bash
set -Eeuo pipefail

# ./run-multi-servers.sh <10|20|50> [seconds] [total-tps]
NODES="${1:-}"
DURATION="${2:-20}"
TOTAL_RATE="${3:-10000}"
case "$NODES" in 10|20|50) ;; *) echo "usage: $0 <10|20|50> [seconds] [total-tps]" >&2; exit 2;; esac
[[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || exit 2
[[ "$TOTAL_RATE" =~ ^[1-9][0-9]*$ ]] || exit 2

FAULTS="${ORCA_FAULTS:-0}"
RULE3_BEHAVIOR="${ORCA_RULE3_BEHAVIOR:-mixed}"
[[ "$FAULTS" =~ ^[0-9]+$ ]] || { echo "ORCA_FAULTS must be a non-negative integer" >&2; exit 2; }
case "$RULE3_BEHAVIOR" in mixed|silent|participate) ;; *) echo "ORCA_RULE3_BEHAVIOR must be mixed, silent, or participate" >&2; exit 2;; esac

REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/Orca-B}"
HOSTS_FILE="${HOSTS_FILE:-deploy/hosts-${NODES}.txt}"
MAX_PARALLEL="${MAX_PARALLEL:-10}"
READY_TIMEOUT="${READY_TIMEOUT:-240}"
TX_SIZE="${TX_SIZE:-512}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8 -o ServerAliveInterval=5 -o ServerAliveCountMax=2)
LOCAL_LOGS="benchmark/logs"

[[ -f "$HOSTS_FILE" ]] || { echo "missing $HOSTS_FILE" >&2; exit 1; }
mapfile -t IPS < <(sed -e 's/#.*//' -e 's/[[:space:]]//g' "$HOSTS_FILE" | awk 'NF')
[[ "${#IPS[@]}" -eq "$NODES" ]] || { echo "$HOSTS_FILE must contain exactly $NODES IPs" >&2; exit 1; }
[[ "$(printf '%s\n' "${IPS[@]}" | sort -u | wc -l)" -eq "$NODES" ]] || { echo "duplicate IP" >&2; exit 1; }
for ip in "${IPS[@]}"; do [[ "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || { echo "invalid IP: $ip"; exit 1; }; done

RATE_SHARE=$(((TOTAL_RATE + NODES - 1) / NODES))
TX_NODES=""
for ip in "${IPS[@]}"; do TX_NODES+="${ip}:3003 "; done
remote() { ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@$1" "$2"; }
wait_batch() { (( $1 % MAX_PARALLEL == 0 )) && wait || true; }

stop_all() {
  local count=0
  echo "[cleanup] stopping $NODES nodes"
  for ip in "${IPS[@]}"; do
    remote "$ip" "tmux kill-session -t orca-client 2>/dev/null || true; tmux kill-session -t orca-primary 2>/dev/null || true; tmux kill-session -t orca-worker 2>/dev/null || true" &
    count=$((count+1)); wait_batch "$count"
  done
  wait || true
}
trap stop_all EXIT INT TERM

echo "nodes=$NODES duration=${DURATION}s total-rate=$TOTAL_RATE per-client=$RATE_SHARE faults=$FAULTS rule3=$RULE3_BEHAVIOR"
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "test -x '$REMOTE_DIR/target/release/node' && test -x '$REMOTE_DIR/target/release/benchmark_client' && test -f '$REMOTE_DIR/deploy/node-${i}.json' && test -f '$REMOTE_DIR/deploy/committee.json' && test -f '$REMOTE_DIR/deploy/parameters.json'"
done

count=0
for ip in "${IPS[@]}"; do
  remote "$ip" "tmux kill-session -t orca-client 2>/dev/null || true; tmux kill-session -t orca-primary 2>/dev/null || true; tmux kill-session -t orca-worker 2>/dev/null || true; cd '$REMOTE_DIR'; rm -rf run/db-primary run/db-worker run/logs; mkdir -p run/logs" &
  count=$((count+1)); wait_batch "$count"
done
wait

for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "cd '$REMOTE_DIR' && tmux new-session -d -s orca-worker \"RUST_LOG=info ./target/release/node -vv run --keys deploy/node-${i}.json --committee deploy/committee.json --parameters deploy/parameters.json --store run/db-worker worker --id 0 |& tee run/logs/worker-${i}-0.log\""
done
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "cd '$REMOTE_DIR' && tmux new-session -d -s orca-primary \"RUST_LOG=info ORCA_FAULTS='$FAULTS' ORCA_RULE3_BEHAVIOR='$RULE3_BEHAVIOR' ./target/release/node -vv run --keys deploy/node-${i}.json --committee deploy/committee.json --parameters deploy/parameters.json --store run/db-primary primary |& tee run/logs/primary-${i}.log\""
done
sleep 6

for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "ss -ltn | grep -q ':3003 '" || { remote "${IPS[$i]}" "tail -100 '$REMOTE_DIR/run/logs/worker-${i}-0.log'"; exit 1; }
done
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "cd '$REMOTE_DIR' && tmux new-session -d -s orca-client \"RUST_LOG=info ./target/release/benchmark_client '${IPS[$i]}:3003' --size '$TX_SIZE' --rate '$RATE_SHARE' --nodes $TX_NODES |& tee run/logs/client-${i}-0.log\""
done

ready=0
for ((elapsed=0; elapsed<READY_TIMEOUT; elapsed+=3)); do
  ready=0; waiting=()
  for i in "${!IPS[@]}"; do
    if remote "${IPS[$i]}" "grep -q 'Start sending transactions' '$REMOTE_DIR/run/logs/client-${i}-0.log'"; then ready=$((ready+1)); else waiting+=("$i"); fi
  done
  echo "${elapsed}s ready=$ready/$NODES waiting=${waiting[*]:-none}"
  (( ready == NODES )) && break
  sleep 3
done
(( ready == NODES )) || { echo "clients not ready" >&2; exit 1; }

sleep "$DURATION"
stop_all
trap - EXIT INT TERM
rm -rf "$LOCAL_LOGS"; mkdir -p "$LOCAL_LOGS"
for i in "${!IPS[@]}"; do
  scp "${SSH_OPTS[@]}" "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/primary-${i}.log" "$LOCAL_LOGS/"
  scp "${SSH_OPTS[@]}" "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/worker-${i}-0.log" "$LOCAL_LOGS/"
  scp "${SSH_OPTS[@]}" "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/client-${i}-0.log" "$LOCAL_LOGS/"
done
cd benchmark
python3 - "$NODES" "$FAULTS" <<'PY'
import sys
from benchmark.logs import LogParser
faults = int(sys.argv[2])
print(LogParser.process("logs", faults=faults).result())
print(f"Parsed {sys.argv[1]} active nodes with faults={faults}")
PY
