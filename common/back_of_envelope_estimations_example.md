# Back of the Envelope Estimations — HLD Problems (Uber SDE2)

> Full HLDs (requirements, components, schemas, DB justification, trade-offs, mermaid diagrams) for each of these problems live in [hld/](hld/README.md).

Cheat sheet of handy constants used throughout:

- 1 day = 86,400 s ≈ 10^5 s
- 100k seconds/day is a common shortcut for QPS math
- Peak QPS ≈ 2× average QPS (use 3× for bursty/consumer systems)
- 1 KB = 10^3 B, 1 MB = 10^6 B, 1 GB = 10^9 B, 1 TB = 10^12 B, 1 PB = 10^15 B
- Replication factor = 3 (default for durable stores)
- Read:Write ratio — read heavy (100:1), write heavy (1:10), balanced (1:1)

---

## 1. Logistics Management System (Fleet + Routing + ETA)

**Assumptions**
- 500k trucks in fleet, 80% active at any moment → 400k live trucks
- Each truck emits GPS ping every 5 s → 12 pings/min
- 2M deliveries/day across the platform
- Average route has 20 waypoints; ETA recomputed every 30 s during active trip
- Location history retained for 90 days; delivery metadata for 5 years

**Estimations**
- Location ingest QPS = 400k / 5 s = **80k pings/sec**, peak **~160k/sec**
- Delivery create QPS = 2M / 86,400 ≈ **25/sec**, peak ~50/sec (low — not the bottleneck)
- ETA compute QPS = 400k active trips / 30 s ≈ **13k/sec**
- Ping payload ≈ 100 B (truck_id, lat, lng, speed, heading, ts)
- Daily location data = 80k × 100 B × 86,400 ≈ **700 GB/day raw**
- 90-day hot storage ≈ 700 × 90 ≈ **63 TB** (×3 replication = ~190 TB)
- Downsample to 1 ping/min for cold tier → ~60 GB/day → ~10 TB/90 days

**Key takeaways**: write-heavy ingest pipeline (Kafka + time-series DB like Cassandra / ScyllaDB / Timescale). ETA service is compute-bound, not storage-bound.

---

## 2. Stock Price Alerting System

**Assumptions**
- 50M users, 20% (10M) set up alerts
- Average 5 rules/user → 50M active rules
- 10k tracked stocks, each publishes a price tick every 1 s during market hours (6.5 h/day)
- 30% of rules fire at least once per day (push notification)
- Retain alert history for 1 year

**Estimations**
- Price tick ingest = 10k ticks/sec (steady during market hours)
- Rule evaluation fan-out: each tick fans out to rules on that stock
  - Avg 50M / 10k = 5k rules per stock → 10k × 5k = **50M rule evals/sec** at market open
  - Solution: partition rules by stock_id; evaluate only rules for the stock that just ticked
- Notifications/day = 50M × 30% = **15M/day** → ~170/s avg, peak ~1–2k/s
- Rule row ≈ 200 B → 50M × 200 B = **10 GB** (fits in Redis / memory)
- Tick history: 10k × 6.5 × 3,600 × 50 B ≈ **12 GB/day**, 1-year = ~4 TB
- Notification log: 15M × 500 B = **7.5 GB/day**, 1-year = ~2.7 TB

**Key takeaways**: Kafka topic per stock symbol (or hashed partition); in-memory rule index keyed by symbol; at-most-once via dedupe key (user_id + rule_id + trigger_window); DLQ for retries.

---

## 3. Order Processing System (Top-K by popularity)

**Assumptions**
- 100M catalog items, 10M DAU
- 20M orders/day, each order has 3 items on average → 60M order-items/day
- Top-K = top 100 items per category, 1k categories
- Popularity window = trailing 24 h, rolling

**Estimations**
- Order QPS = 20M / 86,400 ≈ **230/s**, peak ~700/s
- Item event QPS = **700/s** avg, peak ~2k/s
- Count-min sketch / Redis ZSET per category: 1k × 100 entries × 50 B ≈ **5 MB** (trivially in memory)
- Item detail row = 2 KB; 100M × 2 KB = **200 GB** catalog → single sharded SQL/Mongo cluster
- Top-K reads are ~90% of traffic (read heavy) → cache with ~1s TTL

---

## 4. Uber Eats (General)

**Assumptions**
- 100M MAU, 30M DAU
- 5M orders/day globally, avg 3 items/order
- 1M restaurants, 500k online at peak
- Each user does 10 search/browse actions per session

**Estimations**
- Order write QPS = 5M / 86,400 ≈ **60/s** avg, **peak ~500/s** (dinner spike ~8×)
- Browse/search QPS = 30M × 10 / 86,400 ≈ **3.5k/s**, peak **~15k/s**
- Driver-location ingest: 500k drivers × 1 ping/4s = **125k pings/s**
- Order row ≈ 2 KB → 5M × 2 KB = **10 GB/day** → ~18 TB over 5 years (×3 repl = 55 TB)
- Menu/restaurant data ~10 KB × 1M = **10 GB** (fits in sharded cache)

---

## 5. Uber Eats Restaurant Dashboard (Real-time Metrics)

**Assumptions (given)**
- 10k restaurants
- 100k orders/sec globally (this is the stress number in the prompt)
- Metrics: orders count, top-3 dishes, total $ sold — for last 1 h, 1 day, 7 days
- Dashboard refresh = every 5 s

**Estimations**
- Ingest = **100k orders/sec** → 100k × 500 B = **50 MB/s** → ~4.3 TB/day raw event stream
- Orders per restaurant = 100k / 10k = **10 orders/sec/restaurant** avg
- Sliding-window aggregates per restaurant:
  - 1h window ≈ 36k orders — keep a rolling count + top-K heap
  - 7d window ≈ 6M orders — pre-aggregate into 1-min buckets: 7 × 24 × 60 = 10,080 buckets × 10k restaurants × 200 B ≈ **20 GB** (fits in Redis cluster)
- Dashboard QPS = 10k restaurants / 5 s refresh = **2k reads/sec**

**Key takeaways**: Flink / Kafka Streams for tumbling-window aggregation; store per-minute buckets in Redis; dashboard query just sums N buckets.

---

## 6. Uber Eats — Train PNR Delivery Home Page

**Assumptions**
- 5M train passengers/day in target region
- 5% use the feature → 250k sessions/day
- Each session queries 3 upcoming stations
- 10k train stations indexed

**Estimations**
- Session QPS = 250k / 86,400 ≈ **3/s**, peak **~20/s** (meal windows)
- PNR lookups = 3 × 3/s ≈ 10/s (call railway API; cache aggressively)
- Station → restaurant index: 10k stations × ~50 restaurants × 1 KB = **500 MB** (Redis)
- Delivery feasibility (ETA vs train arrival) is the interesting constraint — not a storage problem

---

## 7. Uber Eats Restaurant Feed Backend (Quad-tree / Geohash)

**Assumptions**
- 1M restaurants worldwide, 500k active
- 30M DAU, each fires ~5 feed queries per session
- Restaurant moves/closes: ~1k updates/day (low write rate)

**Estimations**
- Feed query QPS = 30M × 5 / 86,400 ≈ **1.7k/s**, peak **~7k/s**
- Restaurant record ≈ 5 KB → 1M × 5 KB = **5 GB** (fits in one machine's memory; replicate for HA)
- Quad-tree node count ≈ 1M leaves / 100 per leaf = 10k nodes
- Writes are negligible → quad-tree rebuild/merge done offline; in-memory diff for updates

---

## 8. Uber Driver Location / Heatmap (Dual pipeline)

**Assumptions**
- 5M active drivers globally, 2M online at peak
- Location ping every 4 s
- Real-time tool: last 20 min, 1-min buckets (20 buckets)
- Research tool: hourly buckets, available after 24 h delay, retain 2 years

**Estimations**
- Ingest QPS = 2M / 4 s = **500k pings/sec**, peak ~1M/s
- Ping size = 80 B → 500k × 80 B = **40 MB/s** = **3.5 TB/day**
- Real-time store: 20 min × 500k × 80 B / 4s = 20 × 60 × 500k × 80 B / 4 = **12 GB** hot (Redis / geo-index)
  - Bucketed per 1 min × geohash cell: ~4k geohash cells × 20 buckets × 1 KB = ~80 MB summary
- Research store: hourly aggregates per geohash — tiny compared to raw. Raw 3.5 TB/day × 730 days ≈ **2.5 PB** (S3 / GCS, columnar Parquet)

**Key takeaways**: two pipelines — Kafka → Flink (1-min windows, Redis sink) for real-time; Kafka → S3 → Spark hourly job for research.

---

## 9. Meeting Scheduler (N rooms)

**Assumptions**
- 10k companies, 100 rooms/company avg → 1M rooms
- 50 bookings/room/day → **50M bookings/day**
- Audit log retained 90 days, then purged
- Avg meeting 30 min

**Estimations**
- Booking QPS = 50M / 86,400 ≈ **580/s**, peak **~2k/s** (workday hours concentrate load; effective peak ~5k/s)
- Booking row = 500 B → 50M × 500 B = **25 GB/day**, 90-day audit = **2.3 TB**
- Concurrency: use per-room serialization (row-level lock in SQL, or single-partition writes in a log)

---

## 10. Simplified Twitter (in-memory)

(Already worked in prompt; included for completeness.)
- 300M MAU, 150M DAU, 2 tweets/user/day
- Tweet QPS ≈ **3.5k**, peak ~7k
- Media storage ~30 TB/day → 55 PB over 5 years
- News feed reads ~10× writes → **35k reads/s**, peak ~70k/s

---

## 11. Product Browsing (E-commerce)

**Assumptions**
- 50M MAU, 10M DAU
- 20 product views / user / day → 200M views/day
- 10M products in catalog
- Top-K refresh every 5 min

**Estimations**
- View QPS = 200M / 86,400 ≈ **2.3k/s**, peak **~10k/s**
- Product detail reads: ~90% cache hit — origin QPS ~1k/s
- Catalog size = 10M × 5 KB = **50 GB** (sharded)
- Popularity counter stream = 2.3k events/s → Kafka → Flink → Redis ZSET
- Top-K per category: 1k categories × 100 items × 100 B = **10 MB** (in-memory)

---

## 12. Omegle-like One-to-one Match + Chat

**Assumptions**
- 5M DAU, average session = 5 min, 3 matches/session
- Avg 20 messages per match
- Messages are ephemeral (not persisted beyond session)

**Estimations**
- Concurrent users = 5M × 5 min / (24×60 min) ≈ **17k concurrent**, peak ~50k
- Match QPS = 5M × 3 / 86,400 ≈ **170/s**, peak ~500/s
- Message QPS = 5M × 3 × 20 / 86,400 ≈ **3.5k/s**, peak **~10k/s**
- Matching queue in Redis: ~50k waiting users × 200 B = **10 MB**
- WebSocket fan-in: 50k conns / 10k conns-per-node = **5 nodes** for gateway

---

## 13. Multiplayer Online Chess

**Assumptions**
- 1M DAU, 5 games/user/day → 5M games/day
- Avg 40 moves/game → **200M moves/day**
- Games + moves retained 1 year

**Estimations**
- Move QPS = 200M / 86,400 ≈ **2.3k/s**, peak ~10k/s
- Move row = 50 B (from, to, piece, ts) → 200M × 50 B = **10 GB/day**, 1 yr = **3.6 TB**
- Game metadata: 5M × 500 B = 2.5 GB/day → ~1 TB/yr
- Undo = store moves as append-only log per game; undo = logical pointer move (no delete)

---

## 14. Distributed Notification Service

**Assumptions**
- 200M users across product lines
- 5 notifications/user/day on average → **1B notifications/day**
- Channels: push (70%), email (20%), SMS (10%)
- Retain delivery log for 30 days

**Estimations**
- Notification QPS = 1B / 86,400 ≈ **12k/s**, peak **~50k/s** (marketing blasts)
- Payload ≈ 1 KB → 12k × 1 KB = **12 MB/s** = 1 TB/day
- 30-day log = **30 TB** (×3 repl = 90 TB)
- Per-channel rate limits → per-user sliding window counter in Redis (~200M keys × 100 B = 20 GB)

---

## 15. Proximity Service (Yelp / TripAdvisor)

**Assumptions**
- 200M places globally
- 100M MAU, 20M DAU
- 10 proximity queries/user/day → **200M queries/day**
- Updates: 10k places updated/day (low write)

**Estimations**
- Query QPS = 200M / 86,400 ≈ **2.3k/s**, peak **~10k/s**
- Place record ≈ 2 KB → 200M × 2 KB = **400 GB** catalog
- Geohash index: 200M entries × 50 B = **10 GB** (in memory on sharded nodes)
- Read-heavy → aggressive CDN / edge caching on top-query bucket

---

## 16. Event Processor (Cart / View events)

**Assumptions**
- 500M events/day ('viewed', 'added to cart', etc.)
- 10 event types
- Hot window = last 1 h for leaderboards
- Cold retention = 90 days for analytics

**Estimations**
- Event QPS = 500M / 86,400 ≈ **6k/s**, peak **~25k/s**
- Event size ≈ 500 B → 6k × 500 B = **3 MB/s** = **250 GB/day**
- 90-day cold = **22 TB** (Parquet on S3 ~5–10 TB after compression)
- Redis leaderboard: 1M items × 100 B = 100 MB per leaderboard, 10 boards = 1 GB
- MongoDB detail store: sharded on event_id, 90-day TTL index

---

## 17. Hotel Booking (Search + Reserve, multi-region)

**Assumptions**
- 1M hotels × 100 rooms = 100M rooms
- 50M MAU, 5M DAU
- 20 searches/user + 0.02 bookings/user → 100M searches + 100k bookings/day
- 2 regions active (US, EU), active-active

**Estimations**
- Search QPS = 100M / 86,400 ≈ **1.2k/s**, peak **~5k/s**
- Booking QPS = **~1/s** avg, peak ~20/s — but must be strongly consistent
- Inventory rows = 100M × 200 B = **20 GB** — shard by hotel_id (geo-affinity partitioning)
- Search index (ES): 1M hotels × 5 KB = **5 GB** per region
- Bookings cross-region replicated via CDC; conflicts resolved via per-room serialization (partition owner)

---

## 18. Logging Application (Logger)

**Assumptions**
- 10k services, each emits 100 logs/sec on average → **1M logs/sec**
- Retention: 7 days hot, 90 days warm, 1 year cold
- Avg log line = 500 B

**Estimations**
- Ingest = **1M msgs/s** = **500 MB/s** = **43 TB/day**
- 7-day hot (ES / OpenSearch) = **300 TB** (×3 = ~900 TB) — shard aggressively
- 90-day warm (compressed Parquet on S3, ~10×) = ~400 TB
- 1-year cold = ~1.5 PB compressed
- Query QPS low (~100/s) — optimize writes

---

## 19. Billing Aggregation API (multi-cloud)

**Assumptions**
- 100k customers, 5 cloud providers each
- Each provider emits 1 billing event per resource per hour
- 100 resources/customer avg → 100k × 5 × 100 = **50M events/hour** = **14k events/sec**
- Retention: 2 years

**Estimations**
- Ingest = 14k/s, peak ~30k/s at hour boundary
- Event size = 300 B → 14k × 300 B = 4 MB/s = **350 GB/day**, 2-year ≈ **250 TB**
- Aggregates (daily / monthly / per-service): 100k × 5 × 30 days × 500 B = **7.5 GB/month**
- Read path is dashboard-style → pre-aggregate; real-time accuracy not critical

---

## 20. Backend for Trending Items

**Assumptions**
- 100M items, 50M DAU
- 20 interactions/user/day → **1B interactions/day**
- Trending window = last 1 h; recomputed every 1 min
- Top-K = 100 per category × 1k categories

**Estimations**
- Interaction QPS = 1B / 86,400 ≈ **12k/s**, peak **~50k/s**
- Redis ZINCRBY on sliding window — 60 buckets × 1k categories × 1k items × 100 B = **6 GB**
- Trending read QPS: 50M × 5 views / 86,400 ≈ **3k/s**, cache TTL 30 s → origin ~100/s

---

## 21. Photo Upload + Geo-Tagging (Flickr-like)

**Assumptions**
- 200M MAU, 50M DAU
- 2 photos/user/day → **100M uploads/day**
- Avg photo = 3 MB (raw) + 500 KB (thumb variants)
- 90% have geo-tags
- Retention: forever

**Estimations**
- Upload QPS = 100M / 86,400 ≈ **1.2k/s**, peak **~5k/s**
- Daily storage = 100M × 3.5 MB = **350 TB/day** raw → ~1 PB/day with variants & replication
- 5-year storage ≈ **1.8 EB** (needs tiered storage: S3 hot → Glacier cold)
- Metadata row = 1 KB → 100M × 1 KB = **100 GB/day** → ~180 TB over 5 years
- Geo index: 100M photos × 50 B = **5 GB** per year in geohash index

---

## 22. YouTube View Count System

**Assumptions**
- 2B MAU, 500M DAU
- 10 video views/user/day → **5B views/day**
- 500M total videos, new uploads ~500k/day
- View count shown on page refresh + after watch

**Estimations**
- View event QPS = 5B / 86,400 ≈ **58k/s**, peak **~200k/s**
- Event size ≈ 200 B → 58k × 200 B = **12 MB/s** = **1 TB/day** raw events
- Counter store: 500M videos × 16 B (video_id + counter) = **8 GB** in sharded Redis / ScyllaDB
- Read QPS (view-count display) ≈ 10× event rate = **500k/s** — serve from cache/CDN with 10 s TTL
- Exactness: use approximate counters + periodic reconciliation from Kafka log; HyperLogLog for uniques

**Key takeaways**: write amplification is the story — ingest 200k/s writes, reconcile in batch; reads are CDN-level.

---

## Estimation Checklist (use on any new problem)

1. **Users**: MAU → DAU (typically 30–50%) → concurrent (session length based)
2. **Actions/user/day**: writes, reads, uploads — separate each
3. **QPS**: `actions × users / 86,400`; peak = 2–5× average
4. **Payload size**: per-record bytes; multiply by QPS for bandwidth
5. **Storage**: daily × retention × replication (×3)
6. **Hot vs cold split**: what fits in RAM (Redis), what needs SSD, what goes to object store
7. **Read:write ratio**: decides caching strategy
8. **Bottleneck**: pick the one number the interviewer should remember (e.g., "500k pings/sec" for driver tracking)
