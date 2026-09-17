#!/usr/bin/env bash
# End-to-end smoke test against a running Cairn (make up). Prints PASS or fails.
set -euo pipefail

CAIRN_URL="${CAIRN_URL:-http://localhost:8080}"
BUCKET="smoke-$$"
KEY="dir/blob-$$.bin"
SIZE=$((8 * 1024 * 1024))

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

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
