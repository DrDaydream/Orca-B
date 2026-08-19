#!/usr/bin/env bash
set -Eeuo pipefail

# Usage:
#   ./distribute-deploy-fast.sh <10|20|50>
#
# Examples:
#   ./distribute-deploy-fast.sh 50
#   MAX_PARALLEL=25 ./distribute-deploy-fast.sh 50
#   REMOTE_DIR=/home/ubuntu/Orca-B ./distribute-deploy-fast.sh 50

NODES="${1:-50}"
REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_DIR="${REMOTE_DIR:-$PWD}"
HOSTS_FILE="${HOSTS_FILE:-deploy/hosts-${NODES}.txt}"
DEPLOY_DIR="${DEPLOY_DIR:-deploy}"
MAX_PARALLEL="${MAX_PARALLEL:-20}"

SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=8
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=2
  -o Compression=no
)

die() {
  echo "ERROR: $*" >&2
  exit 1
}

case "$NODES" in
  10|20|50) ;;
  *) die "usage: $0 <10|20|50>" ;;
esac

[[ "$MAX_PARALLEL" =~ ^[1-9][0-9]*$ ]] || \
  die "MAX_PARALLEL must be a positive integer"
[[ -f "$HOSTS_FILE" ]] || die "missing $HOSTS_FILE"
[[ -f "$DEPLOY_DIR/committee.json" ]] || die "missing committee.json"
[[ -f "$DEPLOY_DIR/parameters.json" ]] || die "missing parameters.json"

mapfile -t IPS < <(
  sed -e 's/#.*//' -e 's/[[:space:]]//g' "$HOSTS_FILE" | awk 'NF'
)
[[ "${#IPS[@]}" -eq "$NODES" ]] || \
  die "$HOSTS_FILE must contain exactly $NODES IP addresses"

for ((i=0; i<NODES; i++)); do
  [[ -f "$DEPLOY_DIR/node-${i}.json" ]] || \
    die "missing $DEPLOY_DIR/node-${i}.json"
done

# Quote the destination once for use by the remote shell.
printf -v REMOTE_DEPLOY_Q '%q' "${REMOTE_DIR}/deploy"

FAILED=0
START_SECONDS=$SECONDS

distribute_one() {
  local index="$1"
  local ip="$2"

  # One archive stream and one SSH connection per server.
  tar -C "$DEPLOY_DIR" -cf - \
    committee.json \
    parameters.json \
    "node-${index}.json" |
    ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${ip}" \
      "mkdir -p ${REMOTE_DEPLOY_Q} &&
       tar -C ${REMOTE_DEPLOY_Q} -xf - &&
       chmod 600 ${REMOTE_DEPLOY_Q}/node-${index}.json"

  echo "[$((index + 1))/$NODES] node-${index} -> $ip"
}

echo "Distributing configuration to $NODES servers"
echo "Remote directory: $REMOTE_DIR"
echo "Parallel transfers: $MAX_PARALLEL"

RUNNING=0
for i in "${!IPS[@]}"; do
  distribute_one "$i" "${IPS[$i]}" &
  RUNNING=$((RUNNING + 1))

  if (( RUNNING >= MAX_PARALLEL )); then
    if ! wait -n; then
      FAILED=1
    fi
    RUNNING=$((RUNNING - 1))
  fi
done

while (( RUNNING > 0 )); do
  if ! wait -n; then
    FAILED=1
  fi
  RUNNING=$((RUNNING - 1))
done

(( FAILED == 0 )) || die "one or more configuration transfers failed"

echo "Distribution completed in $((SECONDS - START_SECONDS)) seconds."
