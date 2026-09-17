#!/usr/bin/env bash
# Walks a running Shardwell (make up) through its failure story, printing each
# command before running it and pausing between steps: status, bucket, an
# 8 MiB PUT, locate, GET, stop a holder, GET again, repair back to RF UP
# copies, corrupt a copy on disk, GET again, repair again, restart the node,
# then the metrics that moved and go test -race ./... . -y skips the pauses.
set -euo pipefail

CAIRN_URL="${CAIRN_URL:-http://localhost:8080}"
CAIRN_COMPOSE="${CAIRN_COMPOSE:-docker compose -f $(dirname "$0")/../deploy/docker-compose.yml}"
BUCKET="demo-$$"
KEY="demo.bin"
SIZE=$((8 * 1024 * 1024))
RF=3
YES=0
[ "${1:-}" = "-y" ] && YES=1

TMP="$(mktemp -d)"
STOPPED_NODE=""
cleanup() {
  if [ -n "$STOPPED_NODE" ]; then
    echo "restarting $STOPPED_NODE" >&2
    $CAIRN_COMPOSE start "$STOPPED_NODE" >&2 || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

# step TITLE: announces the next step and waits for enter unless -y.
step() {
  echo
  echo "== $1"
  if [ "$YES" = 0 ]; then
    read -rp "press enter to continue... " _ || true
  fi
}

# run COMMAND: prints the command, then evaluates it.
run() {
  echo "\$ $1"
  eval "$1"
}

sha() { sha256sum "$1" | cut -d' ' -f1; }

locate() { curl -sS "$CAIRN_URL/cluster/locate?bucket=$BUCKET&key=$KEY"; }

# up_holders: prints the node_ids that locate lists as UP holders.
up_holders() {
  locate | grep -o '{[^{}]*}' | grep '"status":"UP"' | sed -n 's/.*"node_id":"\([^"]*\)".*/\1/p'
}

# wait_up_holders N: polls locate until N holders are UP or fails after 90s.
wait_up_holders() {
  for _ in $(seq 1 90); do
    if [ "$(up_holders | wc -l | tr -d ' ')" = "$1" ]; then
      run "locate"
      echo
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $(up_holders | wc -l | tr -d ' ') UP holders after 90s, want $1" >&2
  locate >&2
  exit 1
}

# node_status NODE: prints NODE's status from /cluster/status.
node_status() {
  curl -sS "$CAIRN_URL/cluster/status" | grep -o '{[^{}]*}' | grep "\"node_id\":\"$1\"" \
    | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p'
}

# wait_node_status NODE WANT: polls until NODE reports WANT or fails after 60s.
wait_node_status() {
  for _ in $(seq 1 60); do
    if [ "$(node_status "$1")" = "$2" ]; then
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $1 did not become $2 (is $(node_status "$1"))" >&2
  exit 1
}

# integrity_failures: prints the coordinator's cairn_integrity_failures_total.
integrity_failures() {
  curl -sS "$CAIRN_URL/metrics" | sed -n 's/^cairn_integrity_failures_total //p'
}

# get_matches: GETs the object and checks its sha256 against the upload.
get_matches() {
  run "curl -sS -o $TMP/out $CAIRN_URL/v1/$BUCKET/$KEY"
  run "sha256sum $TMP/out"
  [ "$(sha "$TMP/in")" = "$(sha "$TMP/out")" ] || { echo "FAIL: sha256 mismatch after GET" >&2; exit 1; }
  echo "sha256 matches the upload"
}

cd "$(dirname "$0")/.."

step "cluster status"
run "curl -sS $CAIRN_URL/cluster/status"
echo

step "create bucket $BUCKET"
run "curl -sS -X PUT $CAIRN_URL/v1/$BUCKET"
echo

step "PUT $SIZE random bytes as $KEY"
run "head -c $SIZE /dev/urandom > $TMP/in"
run "sha256sum $TMP/in"
run "curl -sS -i -X PUT --data-binary @$TMP/in $CAIRN_URL/v1/$BUCKET/$KEY"
echo

step "locate: metadata says which nodes hold the blob"
run "locate"
echo
BLOB_ID="$(locate | sed -n 's/.*"blob_id":"\([^"]*\)".*/\1/p')"
HOLDER="$(up_holders | head -1)"
[ -n "$BLOB_ID" ] && [ -n "$HOLDER" ] || { echo "FAIL: locate has no blob_id or UP holder" >&2; exit 1; }

step "GET and compare sha256"
get_matches

step "failover: stop holder $HOLDER"
STOPPED_NODE="$HOLDER"
run "$CAIRN_COMPOSE stop $HOLDER"

step "GET still works with $HOLDER stopped"
get_matches

step "wait for repair: $HOLDER goes DOWN, grace expires, a new copy lands (locate until $RF UP holders)"
wait_node_status "$HOLDER" DOWN
echo "$HOLDER is DOWN"
wait_up_holders "$RF"

step "corruption: flip a payload byte of $BLOB_ID on an UP holder"
CORRUPT="$(up_holders | head -1)"
run "bash scripts/corrupt.sh $CORRUPT $BLOB_ID"

step "GET until the corrupt copy is read: every GET matches, $CORRUPT quarantines it, metadata drops it"
BEFORE="$(integrity_failures)"
for _ in $(seq 1 20); do
  get_matches
  if [ "$(integrity_failures)" -gt "$BEFORE" ]; then
    break
  fi
done
[ "$(integrity_failures)" -gt "$BEFORE" ] || { echo "FAIL: corrupt copy on $CORRUPT was never read" >&2; exit 1; }
echo "cairn_integrity_failures_total: $BEFORE -> $(integrity_failures)"
# The path is inside sh -c so that Git Bash on Windows cannot rewrite it.
VOLUME="$(docker volume ls -q | grep -- "_${CORRUPT}-data\$" | head -1)"
run "docker run --rm -v $VOLUME:/data alpine sh -c 'ls /data/quarantine'"

step "wait for repair to restore $RF UP holders"
wait_up_holders "$RF"

step "start $HOLDER again"
run "$CAIRN_COMPOSE start $HOLDER"
STOPPED_NODE=""
wait_node_status "$HOLDER" UP
echo "$HOLDER is UP"

step "metrics"
run "curl -sS $CAIRN_URL/metrics | grep -E '^cairn_(nodes_up|nodes_total|under_replicated_blobs|pending_deletes|repairs_total|integrity_failures_total|replica_writes_total)'"

step "go test -race ./..."
run "go test -race ./..."

echo
echo "DEMO PASS"
