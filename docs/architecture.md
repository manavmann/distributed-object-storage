# Architecture

## Components

## Write path

## Read path

## Cluster status

`GET /cluster/status` is the coordinator's view of membership, straight from
the in-memory registry (which is loaded from and persisted to the `nodes`
table). Nodes are sorted by id. Times are RFC 3339 in UTC.

```json
{
  "nodes": [
    {
      "node_id": "node1",
      "addr": "http://node1:9000",
      "status": "UP",
      "free_bytes": 53687091200,
      "blob_count": 12,
      "last_seen": "2026-09-16T10:15:04.123Z",
      "status_changed_at": "2026-09-16T10:00:00.000Z"
    }
  ],
  "under_replicated": 0
}
```

- `status` is `UP` or `DOWN`. A node is UP from its first heartbeat until the
  health monitor sees no heartbeat for `CAIRN_HEARTBEAT_TIMEOUT`; the next
  heartbeat flips it back to UP.
- `last_seen` is the last accepted heartbeat; `status_changed_at` is when the
  current status was set. Both come from the coordinator's clock.
- `under_replicated` is the number of objects with fewer live replicas than
  the replication factor. It is always 0 until repair lands.

## Repair and GC

## Failure model
