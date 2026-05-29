# HLD Interview Template

> Fill in the blanks for any system design problem. Follow sections in order.

---

## 1. Clarify Requirements (2-3 mins)

### Functional Requirements

| # | Requirement | Details |
|---|-------------|---------|
| FR1 | | |
| FR2 | | |
| FR3 | | |

**Edge cases to clarify**: duplicates? retries? ordering? idempotency?

### Non-Functional Requirements

| Attribute | Question to Ask | Value |
|-----------|----------------|-------|
| Scale | How many users? DAU? | |
| Latency | Acceptable response time? | e.g., <100ms |
| Consistency | Strong or eventual? | |
| Availability | SLA target? | e.g., 99.99% |
| Durability | Any data loss acceptable? | |

---

## 2. Assumptions & Numbers

| Metric | Value | How |
|--------|-------|-----|
| MAU | | |
| DAU | | ~MAU / 5 or given |
| Requests/user/day | | |
| Avg record size | | KB / MB |
| Retention period | | days / years / forever |
| Peak factor | | 2x-5x of average |

---

## 3. Back-of-the-Envelope Estimation

### QPS

| Metric | Formula | Value |
|--------|---------|-------|
| Write QPS | DAU x writes/user / 86400 | |
| Read QPS | Write QPS x read:write ratio | |
| Peak QPS | Avg QPS x peak factor | |

### Storage

| Metric | Formula | Value |
|--------|---------|-------|
| Daily data | Write QPS x 86400 x record size | |
| Yearly data | Daily x 365 | |
| Total (with replication) | Yearly x replication factor x retention years | |

### Bandwidth (optional)

| Metric | Formula | Value |
|--------|---------|-------|
| Ingress | Write QPS x record size | |
| Egress | Read QPS x record size | |

**Scale classification**: [ ] Read-heavy [ ] Write-heavy [ ] Balanced

---

## 4. API Design

```
# List core APIs (REST / gRPC)

POST   /v1/resource          - Create
GET    /v1/resource/{id}     - Read
PUT    /v1/resource/{id}     - Update
DELETE /v1/resource/{id}     - Delete
GET    /v1/resource?filter=  - Search/List
```

| API | Method | Params | Response | Notes |
|-----|--------|--------|----------|-------|
| | | | | |
| | | | | |

---

## 5. High-Level Architecture

```
Client
  |
  v
API Gateway / Load Balancer
  |
  v
+------------------+     +------------------+
| Service A        |---->| Service B        |
| (sync)           |     | (async via Queue)|
+------------------+     +------------------+
  |         |                    |
  v         v                    v
[Cache]   [Primary DB]      [Message Queue]
(Redis)   (Postgres/         (Kafka/SQS)
           DynamoDB)              |
                                  v
                          [Consumer/Worker]
                                  |
                                  v
                          [Secondary Store]
                          (ES/S3/Analytics)
```

**Design principles applied**:
- Stateless services (horizontally scalable)
- Sync for user-facing reads, async for heavy writes
- Separation of concerns: each service owns its data

---

## 6. Data Model

### Entities

| Entity | Key Fields | Notes |
|--------|-----------|-------|
| | | |
| | | |

### Schema

```sql
-- Primary entity
CREATE TABLE ___ (
    id          UUID PRIMARY KEY,
    ...
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);

-- Index strategy
CREATE INDEX idx___ ON ___(___);
```

### DB Choice & Justification

| Decision | Choice | Why | Why not alternatives |
|----------|--------|-----|---------------------|
| Primary store | | Access pattern: ___, QPS: ___ | |
| Cache | | | |
| Search | | | |
| Queue/Stream | | | |
| Blob storage | | | |

**Partition key**: `___` — chosen because it distributes evenly and matches the primary query.

---

## 7. Deep Dive (pick 1-2)

### Option A: Caching

| Aspect | Decision |
|--------|----------|
| What to cache | |
| Strategy | Cache-aside / Write-through / Write-behind |
| TTL | |
| Invalidation | |
| Hot key handling | |

### Option B: Async Processing (Queue/Stream)

| Aspect | Decision |
|--------|----------|
| What goes async | |
| Tech | Kafka / SQS / RabbitMQ |
| Ordering guarantee | |
| Retry policy | |
| DLQ | |

### Option C: Search

| Aspect | Decision |
|--------|----------|
| Tech | Elasticsearch / Postgres FTS |
| Sync mechanism | CDC / Dual-write |
| Index fields | |
| Query types | Full-text / Faceted / Geo |

### Option D: Real-Time

| Aspect | Decision |
|--------|----------|
| Tech | WebSocket / SSE / Long polling |
| Fan-out strategy | Push / Pull / Hybrid |
| Connection management | |

---

## 8. Scaling Strategy

| Layer | Strategy | Details |
|-------|----------|---------|
| App servers | Horizontal scaling | Stateless behind LB |
| Database | Sharding / Read replicas | Shard key: ___ |
| Cache | Consistent hashing | Redis Cluster |
| Queue | Partitioning | Partition key: ___ |

**Hotspot handling**: ___

---

## 9. Consistency & CAP Trade-offs

| Decision | Choice | Justification |
|----------|--------|---------------|
| CP vs AP | | |
| Consistency model | Strong / Eventual / Causal | |
| Conflict resolution | LWW / CRDT / App-level | |
| Quorum (if applicable) | W=___ R=___ N=___ | |

**Read-your-writes**: How does the user see their own update immediately?

---

## 10. Failure Handling

### Step 1: Failure Detection (Gossip Protocol)

Before handling failures, you must **detect** them. Use a **Gossip Protocol** for decentralized failure detection:

- Every node periodically picks a random peer and exchanges a **heartbeat** (node ID + timestamp + sequence number).
- Each node maintains a **membership list** with last-known heartbeat for every other node.
- If a node's heartbeat hasn't been updated within a **timeout** (e.g., 2x gossip interval), it is marked **suspected**.
- If still unresponsive after a longer window, it is marked **dead** and removed from the ring / routing table.

| Aspect | Detail |
|--------|--------|
| Protocol | Gossip (epidemic protocol) |
| Heartbeat interval | Every 1-2 seconds |
| Suspect threshold | No heartbeat for T seconds |
| Confirmed dead | No heartbeat for 2T-3T seconds |
| Who uses this | Cassandra, DynamoDB, Consul, Redis Cluster |
| Why not centralized? | Single point of failure — gossip scales to thousands of nodes with no coordinator |

**Interview script**: "Before we handle failures, we need to detect them. We use a gossip protocol — each node periodically exchanges heartbeats with random peers. If a node misses heartbeats beyond a threshold, it's marked suspected, then dead. This is decentralized — no single point of failure for detection itself."

### Step 2: Failure Mitigation

| Failure | Mitigation |
|---------|-----------|
| Node crash (detected via gossip) | Replication (N=3), auto-failover, re-route traffic to healthy nodes |
| Network partition | Circuit breaker, fallback response |
| Duplicate requests | Idempotency key on write APIs |
| Downstream timeout | Retry with exponential backoff + jitter |
| Poison message | Dead letter queue (DLQ) |
| Data corruption | Point-in-time backup, WAL |
| Region outage | Multi-region failover (active-passive / active-active) |

---

## 11. Optimizations & Edge Cases

| Category | Technique | Applied where |
|----------|-----------|---------------|
| Caching | TTL, LRU eviction | |
| Rate limiting | Token bucket / Sliding window | API Gateway |
| Pagination | Cursor-based (not offset) | List APIs |
| Batching | Bulk writes | |
| Hot keys | Salting / Split partitions | |
| Data lifecycle | TTL / Archival to cold storage | |

---

## 12. Advanced (if time permits)

- [ ] **Multi-region**: Active-passive vs active-active, data replication lag
- [ ] **Security**: AuthN/AuthZ, encryption at rest + in transit, input validation
- [ ] **Observability**: Structured logs, distributed tracing, dashboards + alerts
- [ ] **Cost**: Estimate monthly cost for compute, storage, bandwidth

---

## Interview Flow Checklist

```
[  ] 1. Requirements     - Scope it. Don't solve the wrong problem.
[  ] 2. Assumptions      - State numbers. Drive everything from data.
[  ] 3. Estimation       - QPS + Storage. Classify the workload.
[  ] 4. API Design       - Define the contract first.
[  ] 5. Architecture     - Start simple: Client → LB → Service → DB.
[  ] 6. Data Model       - Entities, schema, DB choice with justification.
[  ] 7. Deep Dive        - Pick 1-2 hard parts. Show trade-offs.
[  ] 8. Scaling          - Shard, replicate, cache. Handle hotspots.
[  ] 9. Consistency      - CAP choice. Be explicit.
[  ] 10. Failures        - Retries, idempotency, circuit breakers.
[  ] 11. Optimizations   - Caching, rate limiting, pagination.
```

> **Tip**: Always justify with "because ___". Never just name a technology — explain why it fits *this* workload.
