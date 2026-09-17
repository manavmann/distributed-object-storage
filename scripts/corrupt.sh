#!/usr/bin/env bash
# corrupt.sh NODE BLOB_ID: flips payload byte 0 of BLOB_ID in NODE's data
# volume (the compose service name, e.g. node2), so the node's next read of
# it fails verification. The node keeps running; the file is edited from a
# throwaway alpine container sharing the volume.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <node-service> <blob_id>" >&2
  exit 2
fi
NODE="$1" BLOB_ID="$2"
# The blob file is a 64-byte header followed by the payload.
HEADER_SIZE=64

VOLUME="$(docker volume ls -q | grep -- "_${NODE}-data\$" | head -1)"
[ -n "$VOLUME" ] || { echo "corrupt.sh: no data volume for $NODE" >&2; exit 1; }

docker run --rm -v "$VOLUME:/data" alpine sh -euc '
  f="/data/blobs/$1"
  [ -f "$f" ] || { echo "corrupt.sh: $f not found" >&2; exit 1; }
  byte="$(dd if="$f" bs=1 skip="$2" count=1 2>/dev/null | od -An -tu1 | tr -d " ")"
  [ -n "$byte" ] || { echo "corrupt.sh: $f has no payload" >&2; exit 1; }
  printf "$(printf "\\%03o" $((byte ^ 255)))" | dd of="$f" bs=1 seek="$2" count=1 conv=notrunc 2>/dev/null
  echo "flipped payload byte 0 of $1 on $3 ($byte -> $((byte ^ 255)))"
' sh "$BLOB_ID" "$HEADER_SIZE" "$NODE"
