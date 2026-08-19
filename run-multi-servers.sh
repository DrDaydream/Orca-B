#!/usr/bin/env bash
set -Eeuo pipefail

# Usage:
#   ./run-multi-servers-direct.sh <10|20|50> <duration_seconds> <total_tps>
# Example:
#   ./run-multi-servers-direct.sh 50 300 100000

NODES="${1:-50}"
DURATION="${2:-300}"
TOTAL_RATE="${3:-100000}"

REMOTE_USER="${REMOTE_USER:-ubuntu}"
# Run the script from the repository root. All servers are expected to keep
# the same absolute repository path as the controller.
REMOTE_DIR="${REMOTE_DIR:-$PWD}"
HOSTS_FILE="${HOSTS_FILE:-deploy/hosts-${NODES}.txt}"
LOCAL_LOGS="${LOCAL_LOGS:-benchmark/logs}"
TX_SIZE="${TX_SIZE:-512}"

# Give all SSH commands enough time to reach their servers. Clients then wait
# for the same epoch timestamp and start together.
START_DELAY="${START_DELAY:-30}"

FAULTS="${ORCA_FAULTS:-0}"
RULE3_BEHAVIOR="${ORCA_RULE3_BEHAVIOR:-mixed}"
ADVERSARY_SEED="${ORCA_ADVERSARY_SEED:-0}"

SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=8
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=2
)

case "$NODES" in
  10|20|50) ;;
  *) echo "Usage: $0 <10|20|50> [duration] [total_tps]" >&2; exit 1 ;;
esac

mapfile -t IPS < <(
  sed -e 's/#.*//' -e 's/[[:space:]]//g' "$HOSTS_FILE" | awk 'NF'
)

if [[ "${#IPS[@]}" -ne "$NODES" ]]; then
  echo "$HOSTS_FILE must contain exactly $NODES IP addresses" >&2
  exit 1
fi

remote() {
  ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@$1" "$2"
}

stop_nodes() {
  echo "Stopping clients, primaries, and workers..."
  for ip in "${IPS[@]}"; do
    remote "$ip" \
      "tmux kill-session -t orca-client 2>/dev/null || true;
       tmux kill-session -t orca-primary 2>/dev/null || true;
       tmux kill-session -t orca-worker 2>/dev/null || true" &
  done
  wait || true
}

trap stop_nodes INT TERM

# Split total TPS exactly across all clients.
BASE_RATE=$((TOTAL_RATE / NODES))
REMAINDER=$((TOTAL_RATE % NODES))
RATES=()
TX_NODES=""
for ((i=0; i<NODES; i++)); do
  RATES+=("$((BASE_RATE + (i < REMAINDER ? 1 : 0)))")
  TX_NODES+="${IPS[$i]}:3003 "
done

echo "Nodes: $NODES"
echo "Duration: ${DURATION}s"
echo "Total input rate: $TOTAL_RATE TPS"
echo "Per-client rates: ${RATES[*]}"
echo "Faults: $FAULTS; adversary seed: $ADVERSARY_SEED; behavior: $RULE3_BEHAVIOR"

echo "[1/6] Cleaning previous run..."
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "
    tmux kill-session -t orca-client 2>/dev/null || true
    tmux kill-session -t orca-primary 2>/dev/null || true
    tmux kill-session -t orca-worker 2>/dev/null || true
    cd '${REMOTE_DIR}'
    rm -rf -- run/db-primary run/db-worker run/logs
    mkdir -p run/logs
  " &
done
wait

echo "[2/6] Starting all workers concurrently..."
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "
    cd '${REMOTE_DIR}'
    tmux new-session -d -s orca-worker \
      \"RUST_LOG=info ./target/release/node -vv run \
      --keys deploy/node-${i}.json \
      --committee deploy/committee.json \
      --parameters deploy/parameters.json \
      --store run/db-worker \
      worker --id 0 2>&1 | tee run/logs/worker-${i}-0.log\"
  " &
done
wait

echo "[3/6] Starting all primaries concurrently..."
for i in "${!IPS[@]}"; do
  remote "${IPS[$i]}" "
    cd '${REMOTE_DIR}'
    tmux new-session -d -s orca-primary \
      \"RUST_LOG=info \
      ORCA_FAULTS='${FAULTS}' \
      ORCA_RULE3_BEHAVIOR='${RULE3_BEHAVIOR}' \
      ORCA_ADVERSARY_SEED='${ADVERSARY_SEED}' \
      ./target/release/node -vv run \
      --keys deploy/node-${i}.json \
      --committee deploy/committee.json \
      --parameters deploy/parameters.json \
      --store run/db-primary \
      primary 2>&1 | tee run/logs/primary-${i}.log\"
  " &
done
wait

echo "[4/6] Dispatching all clients concurrently..."
START_AT_MS=$(( $(date +%s%3N) + START_DELAY * 1000 ))
CLIENT_TIMEOUT=$((DURATION + START_DELAY + 30))

for i in "${!IPS[@]}"; do
  CLIENT_COMMAND="while [ \"\$(date +%s%3N)\" -lt '${START_AT_MS}' ]; do sleep 0.05; done
timeout '${CLIENT_TIMEOUT}s' ./target/release/benchmark_client \
  '${IPS[$i]}:3003' \
  --size '${TX_SIZE}' \
  --rate '${RATES[$i]}' \
  --nodes ${TX_NODES} \
  2>&1 | tee 'run/logs/client-${i}-0.log'"

  printf -v QUOTED_CLIENT_COMMAND '%q' "$CLIENT_COMMAND"
  remote "${IPS[$i]}" \
    "cd '${REMOTE_DIR}' && tmux new-session -d -s orca-client ${QUOTED_CLIENT_COMMAND}" &
done
wait

echo "All clients will start at epoch ${START_AT_MS} ms."
echo "Waiting ${START_DELAY}s for the common start, then running ${DURATION}s..."
sleep "$((START_DELAY + DURATION))"

echo "[5/6] Stopping the experiment..."
stop_nodes
trap - INT TERM

echo "[6/6] Downloading logs and printing summary..."
rm -rf -- "$LOCAL_LOGS"
mkdir -p -- "$LOCAL_LOGS"

for i in "${!IPS[@]}"; do
  scp "${SSH_OPTS[@]}" \
    "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/primary-${i}.log" \
    "$LOCAL_LOGS/" &
  scp "${SSH_OPTS[@]}" \
    "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/worker-${i}-0.log" \
    "$LOCAL_LOGS/" &
  scp "${SSH_OPTS[@]}" \
    "${REMOTE_USER}@${IPS[$i]}:${REMOTE_DIR}/run/logs/client-${i}-0.log" \
    "$LOCAL_LOGS/" &
done
wait

PYTHONPATH=benchmark python3 - "$LOCAL_LOGS" "$FAULTS" <<'PY'
import sys
from benchmark.logs import LogParser

logs = sys.argv[1]
faults = int(sys.argv[2])
parser = LogParser.process(logs, faults=faults)
print(parser.result())
PY

echo "Experiment completed."
