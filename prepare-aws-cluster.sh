#!/usr/bin/env bash
set -Eeuo pipefail

NODES="${1:?usage: $0 <10|20|50>}"
case "$NODES" in 10|20|50) ;; *) exit 2;; esac
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/Orca-B}"
HOSTS_FILE="${HOSTS_FILE:-deploy/hosts-${NODES}.txt}"
mapfile -t IPS < <(awk 'NF && $1 !~ /^#/ {print $1}' "$HOSTS_FILE")
[[ "${#IPS[@]}" -eq "$NODES" ]] || { echo "expected $NODES hosts" >&2; exit 1; }

mkdir -p deploy
for ((i=0; i<NODES; i++)); do
  ./target/release/node generate_keys --filename "deploy/node-${i}.json"
done
chmod 600 deploy/node-*.json

python3 - "$HOSTS_FILE" <<'PY'
import json
import sys
from pathlib import Path

ips = [x.split('#', 1)[0].strip() for x in Path(sys.argv[1]).read_text().splitlines()]
ips = [x for x in ips if x]
authorities = {}
for i, ip in enumerate(ips):
    key = json.loads(Path(f"deploy/node-{i}.json").read_text())
    authorities[key["name"]] = {
        "primary": {
            "primary_to_primary": f"{ip}:3000",
            "worker_to_primary": f"{ip}:3001",
            "aba_to_aba": f"{ip}:3005",
        },
        "stake": 1,
        "workers": {"0": {
            "primary_to_worker": f"{ip}:3002",
            "transactions": f"{ip}:3003",
            "worker_to_worker": f"{ip}:3004",
        }},
    }
Path("deploy/committee.json").write_text(json.dumps({"authorities": authorities}, indent=4))
parameters = {
    "batch_size": 500000,
    "gc_depth": 50,
    "header_size": 1000,
    "max_batch_delay": 200,
    "max_header_delay": 200,
    "sync_retry_delay": 10000,
    "sync_retry_nodes": 3,
}
Path("deploy/parameters.json").write_text(json.dumps(parameters, indent=4))
PY

for i in "${!IPS[@]}"; do
  target="${REMOTE_USER}@${IPS[$i]}"
  ssh "$target" "mkdir -p '$REMOTE_DIR/deploy'"
  scp "deploy/node-${i}.json" deploy/committee.json deploy/parameters.json "$target:$REMOTE_DIR/deploy/"
done

expected="$(sha256sum deploy/committee.json | awk '{print $1}')"
for ip in "${IPS[@]}"; do
  actual="$(ssh "${REMOTE_USER}@${ip}" "sha256sum '$REMOTE_DIR/deploy/committee.json'" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]] || { echo "$ip committee mismatch" >&2; exit 1; }
done
echo "prepared $NODES nodes, committee sha256=$expected"
