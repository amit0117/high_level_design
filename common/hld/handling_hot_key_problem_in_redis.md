# Redis Geo + Hot Key (Quick Revision Notes)

---

## 1. Hot Key (General)

### Problem

* One Redis key gets too many requests → bottleneck

### Solutions

* **Sharding**

  ```
  user:123:0 ... user:123:N
  shard = hash(id) % N
  ```
* **Read replicas** → scale reads
* **Caching (TTL)** → reduce repeated hits
* **Split by dimension** (time/user/region)

👉 Core idea:

> Break 1 hot key → multiple smaller keys

---

## 2. Geo Partitioning (Baseline)

### Store drivers:

```
drivers:geo:<geohash>
```

Example:

```
drivers:geo:tdr5xy
drivers:geo:tdr6ab
```

* Each geohash = one region
* Driver stored in **only one geohash**

---

## 3. Geo Hot Key Problem

### ❌ Bad

```
drivers:active:city
```

### ⚠️ Problem

```
drivers:geo:tdr5ab  ← too many drivers (airport)
```

---

## 4. Solution: Shard Hot Geohash

```
drivers:geo:tdr5ab:0
drivers:geo:tdr5ab:1
drivers:geo:tdr5ab:2
```

### Shard logic:

```
shard = hash(driver_id) % N
```

---

## 5. Key Clarifications

* Geohash → **spatial partition**
* Shard (`:0, :1`) → **load partition**

👉 Both are different

---

## 6. Why Multiple Geohashes in Read?

> Circle (search) ≠ square (geohash)

* Must query center + neighbors
* Avoid missing nearby drivers

---

## 7. End-to-End Flow

### Driver Update

```
(lat, lng) → geohash
→ store in:
drivers:geo:<geohash>[:shard]
```

---

### User Request

1. Compute geohash
2. Find neighbors
3. Query Redis
4. Merge + sort
5. Assign driver

---

## 8. Example: Non-Hot vs Hot Area

---

### ✅ Non-Hot Area

```
geohash = tdr5xy
```

#### Drivers:

```
driver_A → drivers:geo:tdr5xy
driver_B → drivers:geo:tdr5xy
```

#### Query:

```
GEOSEARCH drivers:geo:tdr5xy COUNT 5
```

#### Result:

```
driver_A (0.6 km)
driver_B (1.2 km)
```

👉 Assign: `driver_A`

---

### 🔥 Hot Area (Airport)

```
geohash = tdr5ab
```

#### Drivers:

```
driver_C → drivers:geo:tdr5ab:1
driver_D → drivers:geo:tdr5ab:2
driver_E → drivers:geo:tdr5ab:0
```

#### Query:

```
drivers:geo:tdr5ab:0
drivers:geo:tdr5ab:1
drivers:geo:tdr5ab:2
```

#### Result:

```
driver_C (0.5 km)
driver_E (0.7 km)
driver_D (0.9 km)
```

👉 Assign: `driver_C`

---

## 9. Redis Internals

* Uses **Sorted Set (ZSET)**
* Score = geohash encoding
* Query = range scan

### Binary Search?

* Internal optimization only
* Not your design concern

---

## 10. Precision vs Geohash Length

| Precision | Example | Size    |
| --------- | ------- | ------- |
| 4         | tdr5    | ~20 km  |
| 5         | tdr5x   | ~2–5 km |
| 6         | finer   | ~1 km   |
| 7         | finer   | ~150 m  |

---

## 11. Final Mental Model

* Write → **1 geohash (1 shard if hot)**
* Read → **multiple geohashes (all shards if hot)**
* Merge → pick nearest

---

## One-line Interview Answer

> “We partition drivers using geohash keys for spatial locality. For hot regions, we shard further using hash(driver_id). Reads fan out across neighboring geohashes and shards, then results are merged to pick the nearest driver efficiently.”
