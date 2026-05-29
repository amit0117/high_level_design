# Databases Cheat Sheet — System Design Interviews

A reference for the databases that show up in High-Level Design (HLD) interviews. For each one: what it is, what it's optimized for, when to pick it, and when NOT to. The last section is a decision matrix you can scan in 30 seconds before answering "why this database?"

---

## 1. Relational Databases (SQL)

### PostgreSQL

**What it is**: Open-source Object-Relational Database Management System (ORDBMS) with full Atomicity Consistency Isolation Durability (ACID) compliance, advanced indexing (B-tree, Hash, GIN, GiST, BRIN), JSON Binary (JSONB) document support, geospatial via PostGIS, full-text search, and Multi-Version Concurrency Control (MVCC) for non-blocking reads.

**Optimized for**: Strong consistency, complex joins, transactions, secondary indexes, rich query language.

**Use when**:
- You need transactions across multiple rows/tables (banking, bookings, orders)
- Schema is well-defined and queries are non-trivial (joins, aggregations)
- You need flexible querying — multiple access patterns on the same data
- Geo queries with PostGIS
- Single-region or single-leader is acceptable; data fits in 1–10 TB per shard
- Examples: Hotel Booking, Meeting Scheduler, Order Processing, Billing Aggregation

**Avoid when**:
- Write throughput exceeds ~50–100k Queries Per Second (QPS) per shard — vertical scaling has limits
- You need active-active multi-region writes (Postgres is single-leader; multi-leader needs BDR or external tooling)
- Schema changes too rapidly (every release alters columns)

**Why not Postgres** (interview script):
- "Single leader bottlenecks writes — for 200k writes/sec we'd need to shard, and once we shard we lose joins anyway, so a horizontally-scalable store is a better fit."
- "Cross-region active-active is painful in Postgres — Cassandra/DynamoDB do this natively."

**Why Postgres over MongoDB**: If you need joins/transactions across User→Order→Payment, Postgres wins. MongoDB's `$lookup` is slow at scale. Postgres + JSONB gives you document flexibility with relational power.

### MySQL

**What it is**: Open-source RDBMS, similar feature set to Postgres but historically simpler, with InnoDB as the default ACID storage engine. Used at huge scale by Facebook, YouTube, Uber.

**Use when**: Same as Postgres. Pick MySQL specifically when:
- You're modeling Uber/Meta-style sharding (Vitess sits on top of MySQL)
- Read replicas are the dominant scaling lever
- You need MyRocks (Log-Structured Merge tree (LSM)) for write-heavy workloads on a SQL engine

**Avoid when**: Same situations as Postgres.

### CockroachDB / Google Spanner / YugabyteDB (NewSQL)

**What it is**: Distributed SQL databases that look like Postgres/MySQL on the wire but shard automatically, replicate via Raft/Paxos, and provide global ACID transactions across regions. Spanner uses TrueTime; Cockroach/Yugabyte use hybrid logical clocks.

**Use when**:
- You need SQL semantics (transactions, joins) AND horizontal scale
- Multi-region strong consistency required (financial ledgers, global inventory)
- Examples: Hotel Booking cross-region, Billing aggregation, anything with strict reconciliation requirements

**Avoid when**:
- You don't need cross-shard transactions — you're paying a latency tax (every write coordinates across regions)
- Write QPS is extreme (>500k/sec) — even NewSQL hurts at that point
- Cost sensitivity — Spanner especially is expensive

---

## 2. Key-Value Stores

### Redis

**What it is**: In-memory data structure store. Supports strings, hashes, lists, sets, sorted sets (ZSET), bitmaps, HyperLogLog (HLL), streams, geospatial. Single-threaded per shard, sub-millisecond latency. Persistence via Append-Only File (AOF) or Redis Database (RDB) snapshots — but persistence is best-effort, not durable like a real database.

**Use when**:
- Caching layer (most common use)
- Rate limiting (sliding-window counters)
- Leaderboards (ZSET with `ZINCRBY` + `ZRANGE`)
- Session store
- Pub/sub for lightweight, ephemeral messaging
- Geospatial proximity queries (`GEOADD`, `GEORADIUS`)
- Real-time aggregation buckets (e.g., last-N-minute counters in dashboards)
- Distributed locks (with caveats — see Redlock debate)
- Examples: Twitter feed cache, Stock alerting rule index, Trending items, Dashboard buckets

**Avoid when**:
- Source of truth for data you can't lose (use it as a cache, not the primary store)
- Working set exceeds RAM by a wide margin — Redis is RAM-bound
- You need complex queries (Redis is access-pattern-driven, not query-driven)

**Why not Redis as primary**:
- "Memory cost — storing 100 TB in RAM is wildly expensive vs. SSD."
- "Durability is best-effort: AOF fsync every second still loses 1 second of data on crash."
- "Failover takes seconds and sentinel/cluster mode has split-brain edge cases."

### Memcached

**What it is**: Pure in-memory key-value cache. Simpler than Redis — strings only, no persistence, no replication, multi-threaded.

**Use when**: You only need a cache, want maximum throughput per node, and don't need data structures.
**Avoid when**: You need anything beyond `GET`/`SET`. In modern designs, Redis usually wins.

### DynamoDB

**What it is**: Amazon's fully-managed NoSQL key-value + document database. Partitioned by hash key (with optional range/sort key), provides single-digit-millisecond latency at any scale, supports global tables (multi-region active-active), Time To Live (TTL), streams (Change Data Capture (CDC)), and on-demand or provisioned capacity.

**What it does**: Stores items (JSON-like documents) keyed by a partition key. Each partition can handle ~3000 reads/sec and ~1000 writes/sec; you scale by adding partitions automatically. Secondary indexes (Global Secondary Index (GSI), Local Secondary Index (LSI)) for alternate access patterns.

**Use when**:
- Predictable, high-volume key-value access (user profile lookups, session store, shopping cart)
- You can model the access patterns up front (single-table design)
- You need multi-region active-active without operating Cassandra yourself
- You're already on Amazon Web Services (AWS) and want managed everything
- Examples: Notification service user preferences, View-count counters, Order metadata, Shopping cart

**Avoid when**:
- Access patterns are exploratory or change frequently (DynamoDB punishes ad-hoc queries)
- You need joins across entities
- Cost matters and you have unpredictable burst traffic (provisioned capacity is wasted; on-demand is expensive)
- You need SQL skills on the team (DynamoDB has a steep modeling learning curve)

**Single-Table Design vs. Exploratory Access**:
- **Single-Table**: Model all entities (Users, Orders) in one table with generic PK/SK. Fetch everything for a screen in one query. Result: <10ms, low cost, rigid schema.
- **Exploratory/Ad-hoc**: If access patterns change frequently, every non-key query becomes a **Scan** (reads every item) → slow, expensive, scales badly.
- **Rule**: If you can't define access patterns up front → don't use DynamoDB.

**Why not DynamoDB** (interview script):
- "We need range scans across customer IDs — DynamoDB GSIs work but become a parallel write amplification problem at our scale."
- "Cross-entity transactions are limited to 100 items — won't fit our use case."

### etcd / ZooKeeper / Consul

**What it is**: Strongly consistent key-value stores using Raft/Zab consensus. Small data, low write throughput, but rock-solid for coordination.

**Use when**:
- Service discovery, configuration, leader election, distributed locks, cluster membership
- Examples: Kubernetes uses etcd; Kafka used to use ZooKeeper for controller election

**Avoid when**: Anything that isn't coordination metadata. These are not application databases.

---

## 3. Wide-Column Stores

### Apache Cassandra

**What it is**: Distributed wide-column store with masterless (peer-to-peer) architecture, tunable consistency (`ONE`/`QUORUM`/`ALL`), Log-Structured Merge tree (LSM) storage engine, partitioned by hash on the partition key. No single point of failure — every node is identical.

**Optimized for**: Massive write throughput, time-series data, multi-datacenter replication, linear horizontal scaling. Reads are slower than writes (LSM compaction overhead).

**Use when**:
- Write-heavy workloads with predictable access patterns
- Time-series data (metrics, logs, IoT events, location pings)
- Multi-region active-active where eventual consistency is OK
- You need to scale writes past what a single-leader SQL database can handle
- Examples: Driver location ingest, Logging application, Stock tick history, YouTube view events, Notification delivery log

**Avoid when**:
- You need joins or transactions
- Read patterns are unknown — Cassandra forces you to model the table per query
- Strong read-your-writes consistency required globally
- Tombstone-heavy workloads (frequent deletes) hurt read performance

**Real-World Examples**:
| Company | Use Case | Why Cassandra |
|---------|----------|---------------|
| **Netflix** | Viewing history, recommendations, billing | Multi-region active-active, linear scaling with subscriber growth |
| **Apple** | iCloud, Maps, iMessage (100k+ nodes) | Petabyte scale, fault tolerance for zero data loss |
| **Uber** | Trip history, GPS pings, fraud detection | Millions of writes/sec from driver locations, 24/7 uptime |
| **Instagram** | Activity feed, DMs | Replaced Redis for cost savings — disk vs RAM at scale |
| **Discord** | Message history (later migrated to ScyllaDB) | Time-series chat logs, write-heavy; hit tombstone issues on deletes |

**Cassandra vs. Alternatives for Write-Heavy Time-Series**:
| Requirement | Winner | Why not the other? |
|-------------|--------|-------------------|
| Millions of writes/sec (GPS, IoT) | **Cassandra** | Spanner's global consistency adds write latency; TimescaleDB hits vertical scaling ceiling |
| Complex analytics (AVG, JOIN with metadata) | **TimescaleDB** | Cassandra can't do math or joins — need Spark on top |
| Durable event log + replay | **Kafka** | Kafka is a transport/buffer, not a queryable DB; pair Kafka→Cassandra for ingest+storage |
| Global strong consistency (finance) | **Spanner** | Cassandra is eventually consistent — risky for money transfers |

**NoSQL "Query-First" Modeling** (applies to both Cassandra & DynamoDB):
- **SQL mindset**: Model your *data* (a "Users" table). **NoSQL mindset**: Model your *questions* (a "GetUsersByCity" table).
- **Cassandra**: Table-per-query. Need users by email AND by phone? Create two tables (`users_by_email`, `users_by_phone`). Won't let you search by a non-key field — that would scan every node.
- **DynamoDB**: Single table + GSIs. Store item with `user_id` as PK, create a GSI on `email`. GSI = a hidden second table managed by AWS, kept in sync automatically. Without a GSI, querying by email forces a full Scan.
- **Key rule**: If you haven't built a table or index for a question, you can't ask it efficiently in either database.

**Why not Cassandra**:
- "We need ad-hoc queries on multiple fields — Cassandra forces a separate table per query and our schema isn't stable yet."
- "Read latency tail (p99) is too high for the user-facing read path; we'd need a cache anyway."

### ScyllaDB

**What it is**: C++ rewrite of Cassandra with a shard-per-core architecture, lower latency, higher throughput per node. API-compatible with Cassandra.

**Use when**: Same use cases as Cassandra but you need lower tail latency or fewer nodes for the same load. Good for view counters, location pings, time-series.

### HBase / Bigtable

**What it is**: Wide-column store on top of HDFS (HBase) or Google's distributed file system (Bigtable). Strong consistency per row, row-key range scans, designed for huge tables (petabytes).

**Use when**:
- Range scans on row keys (e.g., crawl history by domain, time-series by device)
- Tight Hadoop ecosystem integration
- Examples: Web crawler storage, log indexing

**Avoid when**: You don't need range scans — Cassandra/Scylla are easier to operate.

---

## 4. Document Stores

### MongoDB

**What it is**: Document database storing JSON-like (BSON) documents, with a rich query language, secondary indexes, aggregation pipelines, and (since 4.0) multi-document ACID transactions. Sharded by shard key, replica sets for HA.

**Use when**:
- Schema is flexible or evolving (CMS, product catalogs with varied attributes)
- Document model fits naturally (a product with embedded reviews, an order with embedded line items)
- You want SQL-ish query power without rigid schema
- Examples: Product catalog, Event processor cart/view events, Restaurant menus

**Avoid when**:
- You need joins across collections frequently (`$lookup` exists but is slow at scale)
- Strong consistency across documents required
- You're tempted to use it as "SQL with JSON" — Postgres + JSONB is usually a better fit
- Extreme write scale (millions/sec) — MongoDB's Primary-Secondary model bottlenecks writes at one master; Cassandra's peer-to-peer scales better

**Why not MongoDB**:
- "Joins are weak — we need transactional consistency across User, Order, Payment which fits Postgres better."
- "Sharding key choice is permanent and painful to change; our access patterns are still in flux."

**MongoDB vs. Cassandra vs. SQL — Quick Pick**:
| Need | Pick | Why |
|------|------|-----|
| Flexible/nested schema, rapid prototyping | **MongoDB** | Schemaless docs, rich queries |
| Massive write throughput, global availability | **Cassandra** | Peer-to-peer, no master bottleneck |
| ACID transactions, complex joins, money | **PostgreSQL** | Full relational integrity |

### Couchbase / Amazon DocumentDB

Similar tradeoffs to MongoDB; usually picked for managed-service reasons rather than feature differences.

---

## 5. Search Engines

### Elasticsearch / OpenSearch

**What it is**: Distributed full-text search and analytics engine built on Lucene. Inverted indexes for text, BKD trees for numbers/geo, aggregations, near-real-time search (1s refresh).

**Use when**:
- Full-text search (product search, log search, autocomplete)
- Faceted filtering (e-commerce: filter by brand, price range, color)
- Log/metric analytics (the ELK stack: Elasticsearch + Logstash + Kibana)
- Geospatial search at scale
- Examples: Logging application (hot tier), Product browsing search, Hotel search index, Proximity service

**Avoid when**:
- Source of truth for transactional data — Elasticsearch is not Atomicity Consistency Isolation Durability (ACID) compliant; treat it as a derived index
- Heavy update workloads — segment merging gets expensive
- Strong consistency required — refresh is eventual

**Pattern**: Postgres/MySQL holds the source of truth; Change Data Capture (CDC) streams writes to Elasticsearch for search.

---

## 6. Time-Series Databases

### InfluxDB / TimescaleDB / Prometheus

**What it is**: Databases specialized for timestamped data — auto-bucketing, downsampling, retention policies, time-window aggregations.
- **InfluxDB**: purpose-built TSDB, own query language (Flux/InfluxQL)
- **TimescaleDB**: Postgres extension — get SQL + hypertables for time-series
- **Prometheus**: pull-based metrics scraper, designed for monitoring not long-term storage

**Use when**:
- Metrics, sensor data, financial ticks, application performance monitoring
- Queries are time-range scans with aggregations (`last 1h`, `avg over 5min`)
- You need automatic data lifecycle (downsample after 7 days, drop after 90)
- Examples: Driver location heatmap (research tier), Stock tick history, Logging metrics

**Avoid when**: Non-time-series workloads — you're paying for features you don't use.

**TimescaleDB vs. Cassandra for Time-Series — When to pick which**:

| Scenario | Pick | Reason |
|----------|------|--------|
| Smart Building: sensor readings JOINed with tenants, rooms, maintenance | **TimescaleDB** | Need SQL JOINs + `time_bucket()` aggregations + relational metadata |
| Fleet Tracker: 500k trucks sending GPS every second | **Cassandra** | Pure write volume; queries are simple ("path of Truck X yesterday") |
| Trading App: OHLC charts + portfolio balances + transactions | **TimescaleDB** | ACID for money, `time_bucket()` for candlesticks, JOINs for portfolio |
| Security Logs: billions of login attempts globally | **Cassandra** | Write-heavy firehose, no need for joins or complex math |

**Key difference**: TimescaleDB = time-series data that needs relational context (JOINs, ACID). Cassandra = time-series data that is a standalone high-volume log.

---

## 7. Graph Databases

### Neo4j / Amazon Neptune / JanusGraph

**What it is**: Stores nodes and edges with properties; query languages like Cypher/Gremlin. Optimized for traversals (friend-of-friend, shortest path, recommendations).

**Use when**:
- Relationships are the primary query (social graph, fraud rings, knowledge graphs)
- Multi-hop traversals (3+ joins in SQL would be painful)
- Examples: LinkedIn-style "people you may know," fraud detection, recommendation engines

**Real-World: Uber + Neo4j**:
- **Fraud Detection**: Connect accounts by shared device IDs, credit cards, IP addresses. Find fraud rings in milliseconds via graph traversal — SQL would need 5+ self-joins and crash.
- **Route Optimization**: Intersections = nodes, roads = edges. Shortest-path algorithms run natively.

**Why Graph DB beats SQL for deep relationships**:
- SQL: 5-level JOIN on millions of rows → minutes or timeout.
- Neo4j: "Index-free adjacency" — each node physically points to neighbors → 5 hops in milliseconds regardless of DB size.
- **Rule**: If your query is "find all things connected within N hops" and N ≥ 3, use a graph DB.

**Avoid when**:
- Relationships are flat (1-hop joins) — SQL handles this fine
- Write throughput is high — graph DBs don't scale writes as well as KV stores

---

## 8. Object Stores (Blob Storage)

### Amazon Simple Storage Service (S3) / Google Cloud Storage (GCS) / Azure Blob

**What it is**: Massively scalable, durable (11 nines), eventually consistent (now strongly consistent in S3) object storage. Pay per GB stored + per request. Tiering: Standard → Infrequent Access → Glacier.

**Use when**:
- Media (images, videos, audio)
- Backups, archives, data lakes
- Static website assets (paired with a Content Delivery Network (CDN))
- Cold log/event storage (Parquet files)
- Examples: Photo upload, YouTube video chunks, Logging cold tier, Twitter media

**Avoid when**:
- Low-latency random reads of small objects (use Redis or DynamoDB)
- High-frequency mutations (S3 has no in-place updates)

---

## 9. Streaming / Log Stores

### Apache Kafka

**What it is**: Distributed append-only log, partitioned by key, replicated. Producers write, consumers read at their own pace, messages retained for hours/days/forever. Throughput: millions of messages/sec.

**What it does in HLD**:
- Decouples producers from consumers (microservice communication)
- Buffer between ingest and processing (absorbs traffic spikes)
- Source of truth for event-sourced systems
- Replay capability for reprocessing
- Backbone for stream processing (Flink, Kafka Streams, Spark Streaming)

**Use when**:
- Any time you need durable, ordered, replayable events
- Examples: Driver location pings, Stock tick distribution, Order events, Logging pipeline, Notification fan-out

**"Streaming" means**: Data is a continuous river, not a static lake. Apps react to events as they flow — not query a stored pile.

**Avoid when**:
- Low message volume (RabbitMQ is simpler)
- You need per-message ACK semantics with priorities (use RabbitMQ/SQS)
- Request-response pattern (mobile app asking "what's my balance?") — use REST + DB
- Heavy batch transformations (monthly payroll) — use Spark/BigQuery

**Kafka vs. Queue vs. DB — Quick Pick**:
| Need | Pick | Why not the others? |
|------|------|-------------------|
| Millions of events/sec, replay, fan-out to 10 consumers | **Kafka** | RabbitMQ can't handle volume; DB polling doesn't scale |
| Simple task queue (send password reset email) | **RabbitMQ/SQS** | Kafka is overkill — complex to operate |
| "What is my balance?" (request-response) | **REST + DB** | Kafka is async, not designed for synchronous lookups |
| Kafka + Cassandra pattern | Kafka buffers the firehose → Cassandra stores permanently | Kafka is transport, not a queryable archive |

### Amazon Kinesis / Google Cloud Pub/Sub

Managed alternatives to Kafka. Same patterns, less operational burden, less flexibility.

### RabbitMQ

**What it is**: Traditional message broker with exchanges, queues, bindings. Per-message ACK, priorities, dead-letter exchanges, RPC patterns. Push-based to consumers.

**Use when**:
- Task queues (background jobs)
- Per-message ACK required (job processing where each task matters)
- Complex routing rules

**Avoid when**:
- High throughput (>50k msgs/sec) — Kafka scales better
- You need replay/log semantics

### Amazon Simple Queue Service (SQS)

Simpler than Kafka or RabbitMQ. Two flavors: Standard (at-least-once, unordered) and First-In-First-Out (FIFO) (ordered, exactly-once-ish). Use for task queues, fan-out via Amazon Simple Notification Service (SNS) → SQS, dead-letter handling.

---

## 10. Analytics / Data Warehouse

### Snowflake / BigQuery / Redshift / ClickHouse

**What it is**: Columnar databases optimized for OLAP — large scans, aggregations, joins over billions of rows. Separates storage from compute.

**Use when**:
- Business intelligence dashboards
- Ad-hoc analytics on event data
- Cold tier for log/event analysis
- Examples: Billing aggregation reports, Trending analytics, Event processor cold tier

**Avoid when**: OLTP workloads — too slow for single-row reads/writes.

**ClickHouse note**: Open-source, blazingly fast for time-series and event analytics. Uber, Cloudflare, Yandex use it for log/metric analytics. Increasingly common in interviews for "analytics on top of events."

---

## 11. Geospatial Indexes (sometimes a feature, sometimes a DB)

| Tech | Where it lives | What it does |
|------|----------------|--------------|
| **PostGIS** | Postgres extension | R-tree indexes, full GIS query language |
| **Redis Geo** | Redis | Sorted set under the hood; `GEORADIUS` |
| **Elasticsearch geo** | ES | BKD-tree, geo_point, geo_shape |
| **Uber H3** | Library | Hexagonal hierarchical indexing — used by Uber for cells |
| **Google S2** | Library | Spherical cell IDs; used by Foursquare, MongoDB |
| **Geohash** | Library | String prefix encoding of lat/lng; used by many KV stores |
| **Quadtree** | Library | Recursive 2D partitioning; classic interview answer |

**When to use what**:
- Static dataset, infrequent updates → Quadtree or geohash
- Real-time updates + nearby queries → Redis Geo or H3-on-Cassandra
- Rich GIS queries (polygons, intersections) → PostGIS
- Search-style queries with filters → Elasticsearch geo

---

## Decision Matrix — "Which DB for this requirement?"

| Requirement | First pick | Second pick | Don't use |
|-------------|-----------|-------------|-----------|
| Strong ACID transactions, complex joins | PostgreSQL | CockroachDB | DynamoDB |
| 100k+ writes/sec, time-series, eventual OK | Cassandra / Scylla | DynamoDB | Postgres |
| Multi-region active-active | DynamoDB Global Tables / Spanner | Cassandra | Single-leader SQL |
| Sub-ms cache / counters | Redis | Memcached | DynamoDB |
| Leaderboards, sorted real-time data | Redis ZSET | DynamoDB+GSI | Cassandra |
| Full-text / faceted search | Elasticsearch | Postgres FTS | Anything else |
| Graph traversals (3+ hops) | Neo4j | JanusGraph | SQL |
| Blob/media storage | Amazon S3 | GCS / Azure Blob | Any DB |
| Append-only event log | Kafka | Kinesis / Pub/Sub | Database |
| Task queues with priorities | RabbitMQ | SQS | Kafka |
| Time-series metrics | TimescaleDB / InfluxDB | Cassandra | Postgres vanilla |
| OLAP / BI / large scans | ClickHouse / BigQuery / Snowflake | Redshift | OLTP DBs |
| Service discovery / coordination | etcd / ZooKeeper | Consul | Any application DB |
| Geospatial nearby queries | PostGIS / Redis Geo / H3 | Elasticsearch | Naive lat-lng table scan |
| Schema-flexible documents | MongoDB | Postgres + JSONB | Cassandra |
| Coordination / locks / leader election | etcd | ZooKeeper | Redis (with caveats) |

---

## Anti-Patterns To Call Out In Interviews

1. **Using a SQL database as a queue** → Use Kafka/RabbitMQ. Polling tables doesn't scale.
2. **Using Redis as the source of truth** → It's a cache. Pair it with a durable store.
3. **Using Elasticsearch as the source of truth** → It's an index. Source-of-truth lives elsewhere; CDC into ES.
4. **Using MongoDB because "schema-less is easier"** → If you need joins/transactions, you'll regret it. Postgres + JSONB often wins.
5. **Using DynamoDB without modeling access patterns first** → You'll end up with too many GSIs, hot partitions, and unbounded cost.
6. **Sharding too early** → Single Postgres instance handles a surprising amount. Vertical scale + read replicas first.
7. **Using one database for everything** → Polyglot persistence is normal. Picking one store and forcing every requirement into it is a red flag.

---

## Quick Interview Heuristics

- **Read-heavy** → Add a cache (Redis) + read replicas
- **Write-heavy** → Cassandra/Scylla/DynamoDB + Kafka buffer
- **Bursty traffic** → Kafka in front to absorb spikes
- **Strong consistency required** → SQL or NewSQL (Spanner/Cockroach)
- **Multi-region** → Active-passive (Postgres + replicas) OR Active-active (DynamoDB Global Tables / Cassandra)
- **Search/filter** → Elasticsearch on top of source of truth, kept in sync via CDC
- **Real-time aggregations** → Kafka → Flink → Redis (1-min buckets)
- **Cold analytics** → Kafka → S3 (Parquet) → Spark/ClickHouse/BigQuery

When asked "why this database?" — answer in three layers:
1. **What the workload looks like** (read/write ratio, QPS, payload size, consistency need)
2. **Why the chosen store fits those traits** (one or two specific features)
3. **Why two reasonable alternatives don't fit** (be specific — "Cassandra would force per-query tables; we need flexible queries")
