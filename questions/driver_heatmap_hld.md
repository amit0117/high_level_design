# 🚀 Driver Heatmap + Matching — Quick Revision Notes

---

## 1. Cell ID (Geohash)

* `cell_id = truncated geohash`
* Example:
  `te7g9xk → te7g`
* Represents **area**, not exact point

---

## 2. Heatmap vs Driver Matching

| Feature    | Heatmap        | Driver Matching |
| ---------- | -------------- | --------------- |
| Data       | counts         | actual drivers  |
| Model      | event-based    | state-based     |
| Time logic | sliding window | latest state    |
| Accuracy   | approx ok      | must be correct |

👉 **Key line:**
`Matching = state-based, Heatmap = event-based`

---

## 3. Sliding Window vs TTL

### TTL (Simple)

* `cell → count (expires)`
* Approximate
* Sudden drops

### Sliding Window (Accurate)

* `cell → time buckets`
* Smooth decay
* Precise time control

👉
`TTL = coarse sliding window`

---

## 4. Sliding Window Implementation

For each cell:

```
te7g → [bucket1, bucket2, ...]
```

* Maintain `current_sum`
* Add new bucket, remove old
* O(1) update

---

## 5. Where Sliding Window Lives

❌ Not in Redis
✅ In stream processor

```
Kafka → Processor (window) → Redis (final count)
```

👉
`Compute with window, serve from Redis`

---

## 6. Driver Matching Storage

* Store **latest location only**
* Use Redis for fast lookup

```
driver:d1 → {lat, lon} (TTL 10s)
```

👉 TTL = **heartbeat (active driver)**

---

## 7. Source of Truth

* Redis → real-time state
* DB → history
* Kafka → event source (optional)

👉
`Redis serves present, DB stores past`

---

## 8. Geohash for Nearby Search

Steps:

1. Convert location → geohash
2. Find current + 8 neighbors
3. Fetch drivers
4. Compute exact distance

👉 reduces search from `O(N)` → `O(k)`

---

## 9. Why Geohash is Fast

* Groups nearby points via prefix
* Stored in sorted/indexed structure
* Query only relevant buckets

---

## 10. Multi-Resolution (Zoom Levels)

At ingestion:

```
te7g9xk →
  zoom_5 → te
  zoom_7 → te7g
  zoom_10 → te7g9x
```

👉 Precompute limited levels only

---

## 11. Push + Pull Model

### Pull

* Initial load / zoom / pan

### Push

* Incremental updates

👉
`Pull snapshot + Push delta`

---

## 12. WebSocket vs SSE

| Feature   | WebSocket      | SSE         |
| --------- | -------------- | ----------- |
| Direction | bi-directional | server only |
| Use case  | Uber           | stock/news  |

👉
`SSE = stream, WebSocket = interaction`

---

## 13. SSE Behavior

* Long-lived HTTP connection
* Auto-reconnect
* Not truly permanent

---

## 14. Message Routing (WebSocket)

Use envelope:

```
{
  type: "HEATMAP_UPDATE",
  payload: {...}
}
```

👉
`Single connection, multiple logical channels`

---

## 15. Hotspot / Contention Problem

Problem:

```
heatmap:te7g → too many writes
```

---

## Solution: Sharding

```
heatmap:te7g:0
heatmap:te7g:1
...
```

```
shard = hash(driver_id) % N
```

👉
`Shard within the cell, not across cells`

---

## 16. Other Hotspot Fixes

* smaller cells (higher precision)
* batching writes
* caching aggregated results
* limit candidates (matching)

---

## 17. Final Architecture

```
Driver → Kafka
        ↓
  Stream Processor (window)
        ↓
      Redis
        ↓
     API → Client
```

---

## 18. Key One-Liners (Interview Gold)

* `Cell = truncated geohash`
* `Matching is state-based, heatmap is event-based`
* `TTL is coarse sliding window`
* `Compute with sliding window, serve from Redis`
* `Shard within the cell`
* `Pull snapshot + push delta`
* `Redis serves present, DB stores past`

---

# 🔍 Deep Dive (Q&A Revision Section)

---

## 19. Geohash Truncation — Nested, Not Overlapping

Driver's exact location = precision-7 geohash (e.g. `te7g9xk`).
Each zoom level = **prefix** of that same geohash.

```
te7g9xk   ← precision 7  (~150m)
te7g9x    ← precision 6  (~1.2km)
te7g9     ← precision 5  (~5km)
te7g      ← precision 4  (~40km)
```

* One driver → **exactly one cell per zoom level** (prefixes are unique).
* **No duplication** across same-zoom cells.
* Boundary problem (two drivers 50m apart across a cell edge) is solved at **query time** via **1+8 neighbor cells**, not by duplicating writes.

👉 `Storage = nested prefixes. Boundary = solved at read via neighbors.`

---

## 20. Heatmap Counter — Redis Storage

### TTL approach (simple, approximate)

```
KEY:   heatmap:te7g9
VALUE: 47
TTL:   60s
```

* `INCR heatmap:te7g9` on each ping
* Drops to 0 on TTL expiry → cliff

### Sliding window approach (accurate, production)

```
HASH heatmap:te7g9
  bucket:1713520000 → 5
  bucket:1713520010 → 8
  bucket:1713520020 → 12
  bucket:1713520030 → 9
  bucket:1713520040 → 7
  bucket:1713520050 → 6
  total             → 47   ← maintained by Flink for O(1) read
```

* Each field = 10-second bucket
* Flink maintains `total` on every slide → read is just `HGET heatmap:te7g9 total`
* Bucket fields expire via `HEXPIRE` or explicit `HDEL` from Flink

👉 `Sliding = hash of time-buckets per cell + precomputed total.`

---

## 21. Heatmap Counter — Cassandra Storage

For history / replay (NOT live serving):

```sql
CREATE TABLE heatmap_history (
  cell_id       text,       -- "te7g9"
  date_bucket   text,       -- "2026-04-19"
  ts_bucket     timestamp,
  driver_count  int,
  PRIMARY KEY ((cell_id, date_bucket), ts_bucket)
) WITH CLUSTERING ORDER BY (ts_bucket DESC);
```

* **Partition key** = `(cell_id, date_bucket)` → pins one cell-day to one node; daily bucket prevents unbounded partition growth.
* **Clustering key** = `ts_bucket` → time-ordered range scans are a single seek.

👉 `Partition = (entity, date). Clustering = time. Bucketing prevents hotspots.`

---

## 22. Driver Matching — Redis Storage

### Per-driver latest state

```
HASH driver:d1
  lat:     12.9716
  lon:     77.5946
  geohash: te7g9xk
  ts:      1713520120
TTL: 10s   ← heartbeat; absence = driver offline
```

### Per-cell index (for nearby lookup)

Use **ZSET** scored by last-seen timestamp (better than SET — see TTL section):

```
ZSET cell:te7g9xk
  d1 → 1713520120
  d2 → 1713520118
  d3 → 1713520115
```

Reads ignore stale:
```
ZRANGEBYSCORE cell:te7g9xk <now-10s> <now>
```

👉 `Driver = HASH (state) + ZSET per cell (index by last-seen).`

---

## 23. Driver Matching — Cassandra Storage

```sql
CREATE TABLE driver_pings (
  driver_id    text,
  date_bucket  text,
  ts           timestamp,
  lat          double,
  lon          double,
  geohash      text,
  PRIMARY KEY ((driver_id, date_bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC);
```

* **Partition key** = `(driver_id, date_bucket)` → all of one driver's pings for a day on one node.
* **Clustering key** = `ts` → "last 100 pings of d1" is one seek.

Optional secondary table for per-cell queries:
```sql
CREATE TABLE pings_by_cell (
  cell_id, date_bucket, ts, driver_id,
  PRIMARY KEY ((cell_id, date_bucket), ts, driver_id)
);
```

👉 `Cassandra = denormalized — one table per query pattern.`

---

## 24. Update Pattern — "+1 New, −1 Old"

Driver `d1` moves from `te7g9xj` → `te7g9xk`.

### Heatmap (event-based, decrement + increment)

```
HINCRBY heatmap:te7g9xj bucket:<now> -1
HINCRBY heatmap:te7g9xk bucket:<now> +1
```

### Matching (state-based, remove + add)

```
ZREM cell:te7g9xj d1
ZADD cell:te7g9xk <now_ts> d1
HSET driver:d1 geohash=te7g9xk lat=... lon=... ts=<now>
EXPIRE driver:d1 10
```

👉 `Heatmap = ±1 counter delta. Matching = remove+add membership.`

---

## 25. Multi-Zoom Write (Heatmap Only)

On each ping, Flink computes **all zoom prefixes** and updates each:

```
for zoom in [3,4,5,6,7]:
   old_cell = old_geohash[:zoom]
   new_cell = new_geohash[:zoom]
   if old_cell != new_cell:
       emit (heatmap:<old_cell>, -1)
       emit (heatmap:<new_cell>, +1)
```

**Small moves don't cascade:** moving 50m only changes the precision-7 cell. Coarser zooms stay the same → no write amplification at those levels.

```
te7g9xj → te7g9xk:
  zoom 7: changed ✓
  zoom 6: same   ✗ skip
  zoom 5: same   ✗ skip
```

👉 `Write-amplified only where the cell boundary actually crosses.`

---

## 26. Multi-Zoom for Matching? — Usually NO

Unlike heatmap counts, matching stores **driver IDs**, not counts.

| Strategy | Writes | Reads |
|---|---|---|
| Multi-zoom (prefix 3..7) | 5× amplification | simple, but coarse cells balloon to 100K+ drivers |
| **Single precision (prefix 7) + expand neighborhood** | 1× | query 9 → 25 → 49 cells as you widen |

Production choice: **store at one precision, expand at read time.**

👉 `Multi-zoom helps heatmap. For matching, single precision + wider neighborhood.`

---

## 27. Heatmap Query — NO Neighbor Cells Needed

The viewport itself defines the rectangle of cells. Each cell is self-contained.

```
Client: bbox (lat1,lon1)-(lat2,lon2) + zoom=5
    ↓
Server: enumerate all zoom-5 cells inside bbox
    ↓
MGET heatmap:<cell>:total for each
    ↓
Return [{cell, count}, ...]
```

👉 `Heatmap read = viewport cells only. Neighbors only matter for radius search.`

---

## 28. Matching Query — Expand Until Enough Drivers

```
precision = 7
required  = 5
candidates = []

while precision >= MIN_PRECISION (say 4):
    cell      = geohash(user_lat, user_lon, precision)
    neighbors = cell + 8 adjacent at this precision
    candidates = ZUNION(cell:<each>) with freshness filter
    dedupe by driver_id
    if len(candidates) >= required: break
    precision -= 1    # widen

rank by: distance, ETA, rating, fairness, surge
```

**Alternative (production-common):** keep precision fixed, grow neighborhood instead:

```
level 0:  9 cells  (radius ~150m)
level 1: 25 cells  (radius ~400m)
level 2: 49 cells  (radius ~700m)
```

**Always cap expansion** — max precision drop = 2–3, or max radius = 5km. Beyond that, return "no drivers nearby".

👉 `Start fine, widen, cap. Shorten prefix = bigger radius.`

---

## 29. TTL Strategy — Heatmap Simple, Matching Needs a Safety Net

### Heatmap — TTL alone is enough
* Bucket fields auto-expire (`HEXPIRE`) or Flink deletes on slide.
* Eventually consistent; a stale count for a few seconds is harmless.

### Matching — Flink's explicit remove is NOT enough alone

Flink cannot observe silent failures:

| Failure | Without TTL |
|---|---|
| Driver app crashes | Ghost entry forever |
| Network drops | Matched but unreachable |
| Force-quit / battery dead | Permanent phantom |
| Flink job restart / Kafka lag | Stale membership served |

### Two-layer defense (production pattern)

**Layer 1 — Flink explicit bookkeeping (fast path):**
```
ZREM cell:te7g9xj d1
ZADD cell:te7g9xk <now> d1
```

**Layer 2 — TTL / staleness filter (safety net):**
* `driver:d1` HASH has TTL = heartbeat (~10s)
* `cell:<geo>` is a **ZSET scored by last-seen timestamp**
* Reads filter out stale: `ZRANGEBYSCORE cell:<geo> <now-10s> <now>`
* Periodic sweep: `ZREMRANGEBYSCORE cell:<geo> -inf <now-10s>`

👉 `Flink keeps index; TTL/ZSET-staleness cleans up ghosts. Need both.`

---

## 30. Apache Flink — What It Owns

| Responsibility | Flink construct |
|---|---|
| Decode lat/lon → geohash at all zoom prefixes | stateless map |
| Track previous cell per driver | `ValueState<String>` keyed by `driver_id` |
| Emit ±1 deltas per cell per zoom | `KeyedProcessFunction` |
| Aggregate counts in time buckets | `keyBy(cell)` + `SlidingEventTimeWindows` |
| Maintain `total` per cell in Redis | window trigger → Redis sink |
| Write history to Cassandra | parallel sink, exactly-once via 2PC |
| Handle late pings | watermarks + allowed lateness |
| Driver SADD/SREM/HSET updates | side output or separate Flink job |

Job graph:
```
Kafka source
  → keyBy(driver_id)
  → ProcessFunction { compute geohash, diff old↔new, emit (cell, ±1) }
  → keyBy(cell_id)
  → SlidingWindow(60s, 10s).sum()
  → Sink → Redis (live) + Cassandra (history)
```

👉 `Flink = keyed state for "previous cell" + sliding windows for counts.`

---

## 31. Final Rule-of-Thumb Table

| Question | Answer |
|---|---|
| Cell = same geohash duplicated at multiple zooms? | No — nested prefixes, one cell per zoom per driver. |
| Need neighbor cells for heatmap viewport read? | No — bbox defines exact cells. |
| Need neighbor cells for matching? | Yes — 1+8 for boundary + expand outward if sparse. |
| Multi-zoom write for heatmap? | Yes (only zooms whose cell changed). |
| Multi-zoom write for matching? | Usually no — fix precision, expand neighborhood. |
| TTL on heatmap buckets? | Yes — sliding window naturally expires. |
| TTL on driver entries? | Yes — heartbeat + ZSET-by-timestamp safety net. |
| Cap matching expansion? | Yes — max precision drop OR max radius. |
| Flink keeps `total` per cell? | Yes — O(1) reads. |
| Cassandra partition key pattern? | `(entity, date_bucket)` to avoid unbounded partitions. |
| Cassandra clustering key pattern? | `ts` for time-ordered range scans. |

---

🔥 Done — revise this once before interview and you’re solid.
