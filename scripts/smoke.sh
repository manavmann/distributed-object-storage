#!/usr/bin/env bash
# End-to-end smoke test against a running Shardwell (make up): a failover (one
# replica holder is stopped mid-test) and a corruption (one copy is flipped on
# disk, read around, dropped and repaired). Prints PASS or fails.
set -euo pipefail

CAIRN_URL="${CAIRN_URL:-http://localhost:8080}"
CAIRN_COMPOSE="${CAIRN_COMPOSE:-docker compose -f $(dirname "$0")/../deploy/docker-compose.yml}"
BUCKET="smoke-$$"
KEY="dir/blob-$$.bin"
SIZE=$((8 * 1024 * 1024))
RF=3

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

sha() { sha256sum "$1" | cut -d' ' -f1; }

# request METHOD PATH [curl args...]: runs the request, stores the body in
# $TMP/body and the headers in $TMP/headers, prints the status code.
request() {
  local method="$1" path="$2"
  shift 2
  curl -sS -o "$TMP/body" -D "$TMP/headers" -w '%{http_code}' -X "$method" "$@" "$CAIRN_URL$path"
}

expect() {
  local want="$1" got="$2" what="$3"
  if [ "$got" != "$want" ]; then
    echo "FAIL: $what: got HTTP $got, want $want" >&2
    cat "$TMP/body" >&2 || true
    echo >&2
    exit 1
  fi
}

# node_status NODE: prints NODE's status from /cluster/status, or nothing if
# the node is unknown. Each node is one {...} object without nested braces.
node_status() {
  curl -sS "$CAIRN_URL/cluster/status" \
    | grep -o '{[^{}]*}' \
    | grep "\"node_id\":\"$1\"" \
    | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p'
}

# replica_count: locates the object and prints how many holders it has.
replica_count() {
  request GET "/cluster/locate?bucket=$BUCKET&key=$KEY" > /dev/null
  grep -o '"node_id"' "$TMP/body" | wc -l | tr -d ' '
}

# wait_node_status NODE WANT: polls until NODE reports WANT or fails after 60s.
wait_node_status() {
  local node="$1" want="$2"
  for _ in $(seq 1 60); do
    if [ "$(node_status "$node")" = "$want" ]; then
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $node did not become $want (is $(node_status "$node"))" >&2
  curl -sS "$CAIRN_URL/cluster/status" >&2 || true
  echo >&2
  exit 1
}

echo "waiting for $CAIRN_URL/healthz"
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null "$CAIRN_URL/healthz"; then
    break
  fi
  sleep 1
done
curl -sf -o /dev/null "$CAIRN_URL/healthz" || { echo "FAIL: coordinator not healthy" >&2; exit 1; }

echo "create bucket $BUCKET"
expect 201 "$(request PUT "/v1/$BUCKET")" "create bucket"

echo "put $SIZE random bytes as $KEY"
head -c "$SIZE" /dev/urandom > "$TMP/in"
expect 200 "$(request PUT "/v1/$BUCKET/$KEY" --data-binary "@$TMP/in")" "put object"

echo "get and compare sha256"
expect 200 "$(request GET "/v1/$BUCKET/$KEY")" "get object"
[ "$(sha "$TMP/in")" = "$(sha "$TMP/body")" ] || { echo "FAIL: sha256 mismatch after GET" >&2; exit 1; }

echo "locate: expect $RF replicas"
expect 200 "$(request GET "/cluster/locate?bucket=$BUCKET&key=$KEY")" "locate object"
REPLICAS="$(grep -o '"node_id"' "$TMP/body" | wc -l | tr -d ' ')"
[ "$REPLICAS" = "$RF" ] || { echo "FAIL: locate shows $REPLICAS replicas, want $RF" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }
HOLDER="$(grep -o '"node_id":"[^"]*"' "$TMP/body" | head -1 | sed 's/.*:"//;s/"$//')"
[ -n "$HOLDER" ] || { echo "FAIL: locate has no node_id" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }

echo "failover: stop holder $HOLDER"
STOPPED_NODE="$HOLDER"
$CAIRN_COMPOSE stop "$HOLDER"

echo "get with $HOLDER stopped and compare sha256"
expect 200 "$(request GET "/v1/$BUCKET/$KEY")" "get object during failover"
[ "$(sha "$TMP/in")" = "$(sha "$TMP/body")" ] || { echo "FAIL: sha256 mismatch after GET during failover" >&2; exit 1; }

echo "wait for $HOLDER to be DOWN"
wait_node_status "$HOLDER" DOWN

echo "start $HOLDER and wait for UP"
$CAIRN_COMPOSE start "$HOLDER"
STOPPED_NODE=""
wait_node_status "$HOLDER" UP

echo "corruption: flip a payload byte on $HOLDER"
expect 200 "$(request GET "/cluster/locate?bucket=$BUCKET&key=$KEY")" "locate object"
BLOB_ID="$(sed -n 's/.*"blob_id":"\([^"]*\)".*/\1/p' "$TMP/body")"
[ -n "$BLOB_ID" ] || { echo "FAIL: locate has no blob_id" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }
bash "$(dirname "$0")/corrupt.sh" "$HOLDER" "$BLOB_ID"

echo "get until the corrupt copy is found: every GET must match, locate must drop to $((RF - 1))"
for _ in $(seq 1 20); do
  expect 200 "$(request GET "/v1/$BUCKET/$KEY")" "get object with a corrupt copy"
  [ "$(sha "$TMP/in")" = "$(sha "$TMP/body")" ] || { echo "FAIL: sha256 mismatch after GET with a corrupt copy" >&2; exit 1; }
  if [ "$(replica_count)" = "$((RF - 1))" ]; then
    break
  fi
done
[ "$(replica_count)" = "$((RF - 1))" ] || { echo "FAIL: locate still shows $(replica_count) replicas after reads, want $((RF - 1))" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }
grep -q "\"node_id\":\"$HOLDER\"" "$TMP/body" && { echo "FAIL: $HOLDER still listed as a holder after serving a corrupt copy" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }

echo "wait for repair to restore $RF replicas"
for _ in $(seq 1 60); do
  if [ "$(replica_count)" = "$RF" ]; then
    break
  fi
  sleep 1
done
[ "$(replica_count)" = "$RF" ] || { echo "FAIL: locate shows $(replica_count) replicas after repair, want $RF" >&2; cat "$TMP/body" >&2; echo >&2; exit 1; }

echo "quarantine on $HOLDER holds exactly one file for $BLOB_ID"
# Earlier runs leave their deleted copies in quarantine/ too, so only this
# blob's entries count. A repaired copy landing back on $HOLDER lives under
# blobs/, not quarantine/, so exactly one entry is expected.
# The path is inside sh -c so that Git Bash on Windows cannot rewrite it.
QUARANTINED="$(docker run --rm -v "$(docker volume ls -q | grep -- "_${HOLDER}-data\$" | head -1):/data" alpine sh -c 'ls /data/quarantine' | grep "^$BLOB_ID" || true)"
[ "$QUARANTINED" = "$BLOB_ID" ] || { echo "FAIL: quarantine/ on $HOLDER holds [$QUARANTINED] for $BLOB_ID, want exactly $BLOB_ID" >&2; exit 1; }

echo "head"
expect 200 "$(request HEAD "/v1/$BUCKET/$KEY" --head)" "head object"
grep -qi "^content-length: $SIZE" "$TMP/headers" || { echo "FAIL: HEAD Content-Length != $SIZE" >&2; cat "$TMP/headers" >&2; exit 1; }

echo "list"
expect 200 "$(request GET "/v1/$BUCKET/")" "list objects"
grep -q "\"$KEY\"" "$TMP/body" || { echo "FAIL: listing does not contain $KEY" >&2; cat "$TMP/body" >&2; exit 1; }

echo "delete"
expect 204 "$(request DELETE "/v1/$BUCKET/$KEY")" "delete object"

echo "get after delete"
expect 404 "$(request GET "/v1/$BUCKET/$KEY")" "get deleted object"

echo PASS
