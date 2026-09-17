# Failure model

What Shardwell does when things break, one failure class per section. Each
section names the detection point, what every layer does about it, what is
never done, how to reproduce it, and the outcome when the failure exhausts
every copy.

## Silent corruption of a blob on disk

A blob file's payload changes under the node: bit rot, a bad sector, a
filesystem bug, or `scripts/corrupt.sh`. The node's process is fine, its
heartbeats keep coming, and metadata still lists it as a holder.

**Detection point.** Corruption is only detected on read. Every blob file
carries a header with the payload's length and SHA-256; `storage.Store.Read`
verifies the whole payload against that header before a single byte is
served, on every GET and HEAD. Nothing scrubs blobs in the background, so a
corrupt copy that nobody reads stays undetected until a client read or a
repair copy visits it.

**What each layer does.**

1. *Storage node.* A header or checksum failure moves `blobs/<id>` to
   `quarantine/<id>` (a numeric suffix if that name is taken) and answers
   `500 {"error":{"code":"corrupt"}}`. The node's next heartbeat reports
   one blob fewer: `blob_count` is the number of files under `blobs/`.
2. *nodeclient.* A 5xx whose code is `corrupt` is `ErrIntegrity`, distinct
   from `ErrUnavailable` (node gone) and `ErrNotFound` (node never had it).
3. *Read path* (`replication.Reader`). On `ErrIntegrity` the coordinator
   logs `integrity_failure` with `blob_id` and `node_id`, deletes that
   node's `replicas` row and moves on to the next holder: UP holders in
   random order, then DOWN ones. The client sees the intact copy with no
   error. A node that is merely unreachable or lacks the blob is skipped
   without touching metadata; only a node that has itself condemned its
   copy loses its row.
4. *Repair* (`repair.Worker`). Once the row is gone the blob has fewer than
   RF UP holders, so the next tick copies it from a random UP holder onto
   the first-ranked healthy non-holder, often the very node that
   quarantined it, and inserts the new row only if the object still
   references the blob. If the source the worker picks turns out to be
   corrupt, the worker drops that row too (same `integrity_failure` event)
   and retries from another source on the next tick. `/cluster/status`
   reports the blob in `under_replicated` until repair completes.

**What is never done.** A corrupt file is never served, not even partially,
and never deleted: `quarantine/` is append-only and nothing in the system
reclaims it. The node does not report corruption to the coordinator on its
own and the coordinator never guesses at holders: the row is dropped only
after the node returned `corrupt` for that blob. Repair never copies from a
DOWN node and never re-replicates a blob whose only copies are DOWN.

**Reproduce.** Against `make up`:

```
BUCKET=demo; KEY=k
curl -X PUT localhost:8080/v1/$BUCKET
curl -X PUT --data-binary @somefile localhost:8080/v1/$BUCKET/$KEY
curl "localhost:8080/cluster/locate?bucket=$BUCKET&key=$KEY"   # pick a node_id and the blob_id
scripts/corrupt.sh node2 <blob_id>                              # flip payload byte 0 in node2's volume
curl -o /dev/null localhost:8080/v1/$BUCKET/$KEY               # repeat until node2 is visited
curl "localhost:8080/cluster/locate?bucket=$BUCKET&key=$KEY"   # node2 gone; back to 3 after ~CAIRN_REPAIR_INTERVAL
docker compose -f deploy/docker-compose.yml logs coordinator | grep -E 'integrity_failure|repair_completed'
```

`make smoke` runs exactly this: PUT, corrupt one holder, GET until the
digest-checked read drops the holder, wait for repair to restore three
replicas, and check the holder's `quarantine/` has the file.

**When every copy is corrupt.** A read visits each UP holder, each one
quarantines its copy and loses its row, and the client gets
`503 NoHealthyReplica`, and so does every later GET or HEAD. The object stays
in metadata: listing still shows it, `under_replicated` counts it, and every repair
tick logs `repair_skipped reason=no_source` for it. The bytes are gone from
the cluster's point of view, but the three quarantined files are on disk
for an operator to inspect or recover from by hand. Overwriting the key
with a new PUT allocates a fresh blob; the quarantined files are untouched.
