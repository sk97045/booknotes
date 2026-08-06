# Design a Logging Service

> **Prompt:** Design a centralized, multi-tenant logging platform (think Datadog Logs / CloudWatch Logs). Applications embed a lightweight SDK that emits structured log entries; operators search those logs by time, severity, service, user, and keyword within seconds of emit.

---

## 1. Requirements

The whole design turns on one asymmetry: **the write path must never block production code, but the write path is also where 500K events/s of pressure lives.** Everything downstream (Kafka, indexing, Elasticsearch) is a consequence of refusing to let ingest pressure propagate back to the caller.

### Clarifying questions I'd ask

- *"If the backend is down, must every log line survive, or is controlled loss acceptable as long as the caller's hot path never stalls?"* → **Non-blocking is the hard constraint.** SDK retries with backoff, then drops oldest buffered entries. Loss is part of the contract, not just a failure mode.
- *"If one tenant sends a 10× burst, can it slow search for other tenants, or must isolation hold on the ingest path?"* → **Per-tenant rate limits required.** Isolation must hold at ingest, not just query time.
- *"How fresh must search be — seconds, or is a minute OK?"* → **< 30s end-to-end** from SDK emit to queryable. This is a freshness target, not just throughput.
- *"If indexing falls behind, does search error out or return partial results?"* → **Partial results with a freshness caveat.** The platform fails *open* — an error makes it useless exactly during an incident.

### Functional requirements (prioritized)

1. **Emit** — SDK lets apps emit structured logs without blocking their hot path.
2. **Ingest** — centralized backend accepts logs from many instances at high throughput.
3. **Search** — query stored logs by time range + severity, service, user, keyword.

Supporting: durable storage with per-tenant retention; multi-tenant isolation (each tenant sees only its logs, rate-limited independently).

### Non-functional requirements

- **Availability > Consistency on ingest.** This is an **AP write path** — logs are append-only, independent events; there's no cross-record invariant to protect. Losing a few logs beats stalling a caller. *(DDIA Ch. 9 — we deliberately choose availability under partition.)*
- **Throughput:** 500K events/s sustained at peak.
- **Ingest freshness:** p99 emit-to-queryable < 30s.
- **Query latency:** p95 < 2s for time-bounded searches on indexed fields.
- **SDK overhead:** < 5ms p99 on the caller's hot path (async buffering).
- **Tenant isolation:** a noisy neighbor cannot degrade others' ingest or search.

### The one number that forces a decision

I won't front-load BOTE math, but **one estimate drives the storage architecture**:

> 500K events/s × ~500 bytes × 86,400s ≈ **~20 TB raw/day**. Over a 30-day hot window that's ~600 TB uncompressed in the search tier.

That number rules out "just keep everything hot forever" and forces **time-based index rotation + hot/cold tiering**: one index per day so retention is an index *drop* (one API call), not a per-document delete scan. Everything else in the design is throughput and isolation plumbing.

---

## 2. Core Entities

- **LogEntry** — `{ log_id, tenant_id, timestamp (ingest), client_ts, severity, service, user_id, operation, resource, message, fields }`. The unit of ingest and search. `log_id` is server-generated → naturally idempotent for replay.
- **Tenant** — `{ tenant_id, api_key_hash, hot_retention, cold_retention, rate_limit_eps }`. Small, high-integrity, transactional. Lives in Postgres.
- **Batch** — the SDK's flush unit (~100 entries / ~50 KB). Never persisted as an entity; it's the transport granularity that amortizes network cost.

### Access patterns

These four boundaries have **different durability and access profiles**, which is why no single store fits — the entity map above is really a *placement* decision:

- **Ingest hot path** — authenticate API key, resolve `tenant_id`, publish batch to Kafka. **No Elasticsearch write on this path** — the whole point of the durable log is that ingest doesn't wait on indexing.
- **Search** — query Elasticsearch filtered by `tenant_id` + time range + optional severity, service, user, keyword.
- **Single log lookup** — fetch one document by `_id` from Elasticsearch (the detail view behind a clicked search result).
- **Retention cleanup** — list Elasticsearch indexes older than the tenant's hot window, snapshot to S3, then delete the index.
- **Tenant management** — CRUD on the `tenants` table in PostgreSQL for admin operations.

So each entity lands where its access pattern demands: **Kafka** is the ingest boundary (once acked, indexing is guaranteed); **Elasticsearch** is the queryable truth for the hot window; **S3** is the cold truth after hot retention expires; **Postgres** holds the small, high-integrity tenant config that needs transactional CRUD and strong consistency. SDK-side buffered logs are ephemeral and lossy by design — they live in host memory and are *not* part of the durable boundary.

### Storage tradeoffs — Elasticsearch vs. ClickHouse

**Elasticsearch is the right hot-search store** because it combines **inverted indexes** for keyword search with **time-based index rotation** that naturally matches log retention (retention becomes an index drop, not a scan). The alternative is **ClickHouse**, which offers better compression and aggregate-query performance but weaker ad-hoc full-text search. For a **Datadog-like debugging experience** — operators searching by arbitrary fields and keywords — Elasticsearch fits better. **ClickHouse would win if** the workload were primarily dashboard analytics over log counts and latency percentiles rather than interactive debugging. *(This is the same fork drawn out in Deep Dive 2's ES-vs-ClickHouse reasoning.)*

---

## 3. API / System Interface

Two contracts that share **nothing but tenant identity** resolved from the API key. Every request carries `Authorization: Bearer <api_key>`; the gateway resolves it to `tenant_id` and stamps it server-side. **Clients never supply their own `tenant_id`** — identity comes from the token, never the body.

### Ingest — fire-and-forget

```
POST /v1/logs/ingest
Authorization: Bearer <api_key>
Body: [ { timestamp, severity, service, message, fields? }, ... ]

→ 202 Accepted { batch_id }     // durably buffered in Kafka, NOT yet indexed
→ 429 Too Many Requests + Retry-After   // tenant over rate_limit_eps
→ 503 Service Unavailable        // Kafka slow → SDK backs off & retries
```

The **202 is the key semantic**: it means "durably handed off to Kafka," not "indexed." The SDK treats 429 exactly like a transient failure — back off, retry, then drop oldest.

### Search — request/response

```
POST /v1/logs/search          // POST, not GET: complex filters exceed URL limits
Body: { time_range (required, ≤24h default), severity?, service?,
        user_id?, operation?, query?, cursor? }

→ 200 { results: [...], next_cursor, freshness: { indexing_lag_s } }
```

Sorted `timestamp DESC`, cursor-paginated. **Mandatory time range (max 24h)** prevents accidental full-index scans — the single most important guardrail on query cost.

### Supporting

```
GET  /v1/logs/{log_id}                      // canonical detail view
GET  /v1/tenants/{tenant_id}/retention      // admin
PUT  /v1/tenants/{tenant_id}/retention      // takes effect next lifecycle run
```

---

## 4. High-Level Design

### Start naive, then break it

The simplest thing that works: app calls `log()` **synchronously** → app server writes raw text to a flat Postgres table → operators search with `SELECT ... WHERE message LIKE '%term%'`. Correct, explainable — and it dies two ways under load:

1. **Synchronous writes block the caller.** A DB slowdown during a burst propagates latency straight into production request paths. This violates the hard constraint.
2. **`LIKE '%term%'` scans billions of rows.** No inverted index → every keyword search is a full table scan → dashboard queries time out.

Those two failures *are* the design spec: (1) demands a **client-side async buffer** so logging never waits on the network; (2) demands a **durable queue** so a burst doesn't become instant indexing pressure, plus a **specialized search index** so keyword queries don't scan history.

### Architecture

Follow one entry from caller to search: `log()` → SDK ring buffer (returns instantly) → background thread batches into one HTTP POST → **ingest gateway** (auth, resolve tenant, rate-limit) → **Kafka** (durable handoff, partitioned) → **indexing workers** (Kafka consumer group) → bulk-write to **Elasticsearch** daily indexes → operators hit a **separate query API**. A **retention worker** later snapshots expired hot indexes to **S3** and drops them.

<svg viewBox="0 0 900 560" xmlns="http://www.w3.org/2000/svg" font-family="'Comic Sans MS','Segoe Print',cursive" font-size="13">
  <style>
    .box{fill:#fffef7;stroke:#2b2b2b;stroke-width:2;}
    .db{fill:#eef6ff;stroke:#2b2b2b;stroke-width:2;}
    .queue{fill:#fff4e6;stroke:#2b2b2b;stroke-width:2;}
    .cache{fill:#f0ffe6;stroke:#2b2b2b;stroke-width:2;}
    .lbl{fill:#1a1a1a;}
    .arr{stroke:#2b2b2b;stroke-width:2;fill:none;marker-end:url(#ah);}
    .arrd{stroke:#8a5a2b;stroke-width:2;fill:none;stroke-dasharray:6 4;marker-end:url(#ahd);}
    .note{fill:#7a5230;font-style:italic;font-size:11px;}
    .title{font-size:15px;font-weight:bold;fill:#1a1a1a;}
  </style>
  <defs>
    <marker id="ah" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#2b2b2b"/></marker>
    <marker id="ahd" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#8a5a2b"/></marker>
  </defs>

  <text x="20" y="28" class="title">Logging Platform — High-Level Architecture</text>

  <!-- App + SDK -->
  <rect x="20" y="60" width="150" height="90" rx="6" class="box"/>
  <text x="95" y="88" text-anchor="middle" class="lbl">App instance</text>
  <rect x="35" y="100" width="120" height="38" rx="4" class="cache"/>
  <text x="95" y="118" text-anchor="middle" class="lbl" font-size="11">SDK ring buffer</text>
  <text x="95" y="132" text-anchor="middle" class="lbl" font-size="11">(non-blocking)</text>

  <!-- Ingest Gateway -->
  <rect x="245" y="60" width="150" height="90" rx="6" class="box"/>
  <text x="320" y="95" text-anchor="middle" class="lbl">Ingest Gateway</text>
  <text x="320" y="115" text-anchor="middle" class="lbl" font-size="11">auth · resolve tenant</text>
  <text x="320" y="131" text-anchor="middle" class="lbl" font-size="11">rate-limit (429)</text>

  <!-- Kafka -->
  <rect x="470" y="60" width="150" height="90" rx="6" class="queue"/>
  <text x="545" y="95" text-anchor="middle" class="lbl">Kafka</text>
  <text x="545" y="114" text-anchor="middle" class="lbl" font-size="11">durable handoff</text>
  <text x="545" y="130" text-anchor="middle" class="lbl" font-size="11">partitioned</text>

  <!-- Indexing workers -->
  <rect x="470" y="230" width="150" height="80" rx="6" class="box"/>
  <text x="545" y="262" text-anchor="middle" class="lbl">Indexing workers</text>
  <text x="545" y="281" text-anchor="middle" class="lbl" font-size="11">consumer group</text>
  <text x="545" y="297" text-anchor="middle" class="lbl" font-size="11">bulk write</text>

  <!-- Elasticsearch -->
  <path d="M700,230 h150 v75 a75,14 0 0 1 -150,0 v-75 a75,14 0 0 1 150,0" class="db"/>
  <ellipse cx="775" cy="230" rx="75" ry="14" class="db"/>
  <text x="775" y="235" text-anchor="middle" class="lbl">Elasticsearch</text>
  <text x="775" y="270" text-anchor="middle" class="lbl" font-size="11">logs-YYYY-MM-DD</text>
  <text x="775" y="288" text-anchor="middle" class="lbl" font-size="11">(hot, queryable)</text>

  <!-- Query API -->
  <rect x="700" y="400" width="150" height="70" rx="6" class="box"/>
  <text x="775" y="430" text-anchor="middle" class="lbl">Query API</text>
  <text x="775" y="449" text-anchor="middle" class="lbl" font-size="11">tenant_id + time filter</text>

  <!-- Operator -->
  <rect x="470" y="410" width="150" height="50" rx="6" class="box"/>
  <text x="545" y="440" text-anchor="middle" class="lbl">Operator</text>

  <!-- Postgres -->
  <path d="M245,240 h150 v55 a75,12 0 0 1 -150,0 v-55 a75,12 0 0 1 150,0" class="db"/>
  <ellipse cx="320" cy="240" rx="75" ry="12" class="db"/>
  <text x="320" y="244" text-anchor="middle" class="lbl">Postgres</text>
  <text x="320" y="278" text-anchor="middle" class="lbl" font-size="11">tenant metadata</text>

  <!-- Retention worker + S3 -->
  <rect x="700" y="330" width="150" height="45" rx="6" class="box"/>
  <text x="775" y="352" text-anchor="middle" class="lbl" font-size="12">Retention worker</text>
  <text x="775" y="367" text-anchor="middle" class="lbl" font-size="11">snapshot + drop index</text>

  <path d="M245,400 h120 v45 a60,11 0 0 1 -120,0 v-45 a60,11 0 0 1 120,0" class="db"/>
  <ellipse cx="305" cy="400" rx="60" ry="11" class="db"/>
  <text x="305" y="404" text-anchor="middle" class="lbl">S3</text>
  <text x="305" y="435" text-anchor="middle" class="lbl" font-size="11">cold archive</text>

  <!-- Arrows -->
  <path d="M170,105 H243" class="arr"/>
  <text x="180" y="98" class="note">batched POST</text>
  <path d="M395,105 H468" class="arr"/>
  <text x="404" y="98" class="note">publish</text>
  <path d="M545,150 V228" class="arr"/>
  <text x="552" y="195" class="note">consume</text>
  <path d="M620,270 H698" class="arr"/>
  <text x="628" y="263" class="note">bulk index</text>
  <path d="M775,470 V308" class="arr" transform="translate(0,0)"/>
  <path d="M775,398 V310" class="arr"/>
  <text x="782" y="355" class="note">read</text>
  <path d="M620,435 H698" class="arr"/>
  <text x="628" y="428" class="note">search</text>
  <path d="M320,150 V228" class="arrd"/>
  <text x="326" y="195" class="note">cache TTL</text>
  <!-- retention -->
  <path d="M775,308 V328" class="arrd"/>
  <path d="M700,352 H370" class="arrd"/>
  <text x="470" y="345" class="note">archive expired index</text>

  <text x="20" y="530" class="note">solid = correctness-critical path   ·   dashed = optimization / lifecycle path</text>
</svg>

**Four backpressure boundaries** — each *absorbs* pressure instead of propagating it upstream:

1. **SDK buffer** — `log()` is a non-blocking enqueue into a ring buffer; full → drop oldest. Caller never waits.
2. **Ingest gateway** — validates, rate-limits per tenant (429), publishes to Kafka. Kafka slow → 503 → SDK backs off.
3. **Kafka** — durable event log decoupling ingest from indexing; partitioned for parallel consumption. Once acked, indexing is *guaranteed to eventually happen*.
4. **Workers → ES** — bulk-write; commit Kafka offset **only after** a successful ES write, so a crash **replays** rather than loses. Persistent failures → dead-letter topic.

The **query path shares nothing with ingest** except the ES cluster it reads. A search spike doesn't slow ingest; a log burst doesn't block search beyond natural indexing lag. *(DDIA Ch. 11 — Kafka as replayable log; Ch. 12 — derived search index built from that log.)*

**Why Elasticsearch over ClickHouse:** inverted indexes give ad-hoc full-text + arbitrary-field search (the Datadog debugging experience), and time-based index rotation matches retention cleanly. ClickHouse wins on compression and aggregate/dashboard queries but loses on interactive keyword search — the wrong trade for a debugging tool.

---

## 5. Deep Dives

The two probes that earn senior signal: **the SDK buffer contract** (the domain-unique hard part) and **Kafka partition strategy** (whether you scale linearly or hit tenant-shaped hot partitions). ES cluster ops matter operationally but don't change the architecture.

### Deep Dive 1 — The SDK is a lossy buffer *by design*

`log()` sits on the application's hot path. If it blocks for even a few ms during a backend slowdown, it's already failed. So the SDK maintains a **bounded ring buffer**; `log()` is a lock-free append (microseconds) that returns immediately. A background flush thread wakes on **whichever comes first** — count threshold (~100) or time interval (~5s) — drains a batch, POSTs once.

The load-bearing decision is **what happens when the buffer fills during retries**:

- **Drop oldest** → most recent logs survive. ✅ Correct for debugging — you want current context during an active incident.
- Block `log()` → violates the primary contract. ❌
- Drop newest → preserves ancient history at the cost of the logs you actually need right now. ❌

> **The aha:** the SDK guarantees a *non-blocking call*, not *lossless delivery*. The app's job is to stay fast; the observability pipeline's job is best-effort. Confusing those priorities is how a logging SDK causes the outage it was meant to diagnose.

**How I'd say it in the interview:** *"The SDK is a lossy buffer by design. It guarantees `log()` won't block, not that every log survives. If the backend is down five minutes, we lose some logs — fine. What's not fine is adding latency to production requests because logging is waiting on a network call."*

<svg viewBox="0 0 860 470" xmlns="http://www.w3.org/2000/svg" font-family="'Comic Sans MS','Segoe Print',cursive" font-size="12.5">
  <style>
    .life{stroke:#2b2b2b;stroke-width:1.5;stroke-dasharray:4 4;}
    .head{fill:#fffef7;stroke:#2b2b2b;stroke-width:2;}
    .msg{stroke:#2b2b2b;stroke-width:2;fill:none;marker-end:url(#s);}
    .ret{stroke:#8a5a2b;stroke-width:2;fill:none;stroke-dasharray:6 4;marker-end:url(#sd);}
    .lbl{fill:#1a1a1a;} .note{fill:#7a5230;font-style:italic;font-size:11px;}
    .title{font-size:15px;font-weight:bold;} .act{fill:#eef6ff;stroke:#2b2b2b;stroke-width:1.5;}
  </style>
  <defs>
    <marker id="s" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#2b2b2b"/></marker>
    <marker id="sd" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#8a5a2b"/></marker>
  </defs>
  <text x="20" y="26" class="title">Ingest sequence — flush, retry, offset commit</text>

  <!-- lifelines -->
  <g>
    <rect x="40" y="45" width="110" height="34" rx="5" class="head"/><text x="95" y="67" text-anchor="middle" class="lbl">Caller</text>
    <line x1="95" y1="79" x2="95" y2="440" class="life"/>
    <rect x="215" y="45" width="120" height="34" rx="5" class="head"/><text x="275" y="67" text-anchor="middle" class="lbl">SDK buffer</text>
    <line x1="275" y1="79" x2="275" y2="440" class="life"/>
    <rect x="405" y="45" width="120" height="34" rx="5" class="head"/><text x="465" y="67" text-anchor="middle" class="lbl">Gateway</text>
    <line x1="465" y1="79" x2="465" y2="440" class="life"/>
    <rect x="600" y="45" width="90" height="34" rx="5" class="head"/><text x="645" y="67" text-anchor="middle" class="lbl">Kafka</text>
    <line x1="645" y1="79" x2="645" y2="440" class="life"/>
    <rect x="740" y="45" width="100" height="34" rx="5" class="head"/><text x="790" y="67" text-anchor="middle" class="lbl">Worker+ES</text>
    <line x1="790" y1="79" x2="790" y2="440" class="life"/>
  </g>

  <path d="M95,105 H273" class="msg"/><text x="105" y="99" class="note">log() — appends, returns instantly</text>
  <path d="M273,107 H97" class="msg"/><text x="150" y="128" class="note">(no wait)</text>

  <path d="M275,165 H463" class="msg"/><text x="285" y="159" class="note">flush: batched POST /ingest</text>
  <path d="M465,195 H647" class="msg"/><text x="475" y="189" class="note">publish batch</text>
  <path d="M647,197 H467" class="msg"/><text x="475" y="216" class="note">ack (durable)</text>
  <path d="M465,240 H277" class="msg"/><text x="290" y="234" class="note">202 Accepted → advance read ptr</text>

  <text x="405" y="272" class="note">— if Kafka slow / 5xx —</text>
  <path d="M465,295 H277" class="ret"/><text x="285" y="289" class="note">503 → SDK backoff (1s→30s), keep buffering</text>
  <text x="215" y="322" class="note">buffer full during retry → drop OLDEST</text>

  <path d="M645,360 H792" class="msg"/><text x="655" y="354" class="note">consume partition</text>
  <path d="M790,392 L740,392" class="msg"/><text x="600" y="386" class="note">bulk index OK</text>
  <path d="M790,425 H647" class="msg"/><text x="655" y="419" class="note">commit offset AFTER ES success</text>
  <text x="150" y="425" class="note">crash before commit ⇒ replay (dup ok, idempotent by log_id)</text>
</svg>

### Deep Dive 2 — Kafka partition strategy

The partition key decides whether 500K/s scales or whether a big tenant creates a hot partition that bottlenecks *everyone* behind it.

| Strategy | Ordering | Hot-partition risk | Verdict |
|---|---|---|---|
| **By `tenant_id`** | per-tenant total order | ❌ 50K/s tenant → one partition, one consumer, one bottleneck | too coarse |
| **By `hash(tenant_id + service)`** | per tenant-service | ✅ spreads a big tenant across its services | **default choice** |
| **Round-robin** | none | ✅ perfectly even | for the *largest* tenants only |

**Default: `hash(tenant_id + service)`.** Most large tenants run many services, so this spreads their load while preserving ordering *within* a tenant-service pair — which is all debugging needs. Cross-partition global order is reconstructed for free because **ES re-sorts by timestamp at query time**. Round-robin is the escape hatch for a single tenant so large it saturates even that: you sacrifice all partition ordering, but since ES re-sorts anyway, it's usually fine.

**Recovery mechanics (the senior tell):** workers are a consumer group — adding workers auto-rebalances; a crash reassigns partitions and resumes **from the last committed offset**. Offsets commit *only after* a successful ES bulk write → a crash **replays** rather than loses. Replay duplicates are harmless because entries are **idempotent by `log_id`**. A batch that fails retries → **dead-letter topic**, so one poison batch can't block its partition forever.

### Deep Dive 3 — Noisy-neighbor containment (defense in depth)

Rate limiting at the gateway is necessary but not sufficient — a big tenant still shares Kafka and ES capacity downstream:

1. **Gateway** — token bucket per tenant (`rate_limit_eps` from Postgres, cached with short TTL). Over limit → 429.
2. **Indexing workers** — **weighted fair queuing**: track per-tenant counts in the window, throttle tenants over their fair share.
3. **Query side** — per-tenant concurrency limits so an expensive search can't starve neighbors; heavy tenants → dedicated read replicas.
4. **Largest tenants** — hard isolation: dedicated Kafka topic + dedicated ES index. Operationally expensive, so reserved for the top few.

### Deep Dive 4 — Retention & the shared-index TTL conflict

Daily indexes are **shared across tenants with different retention windows** — that's the wrinkle. An hourly retention worker keeps an index alive **as long as any tenant with data in it still needs hot access**; per-tenant retention *within* a live index is enforced at **query time** (search filters out results older than that tenant's hot window). Once every tenant's window has passed, the whole index is snapshotted to S3 (compressed ~5:1, time-partitioned) and dropped. Cold logs aren't directly queryable — restore = re-index a segment into a temporary ES index.

### Deep Dive 5 — Degraded reads & adaptive sampling

- **Fail open:** when indexing lags, search returns **partial results + a freshness caveat** (`indexing_lag_s`) rather than erroring — the platform must work hardest exactly when engineers are debugging.
- **Clock skew:** index by **server-side ingest timestamp** for stable ordering; keep the client timestamp as a separate field. Source clocks drift; results shouldn't.
- **Adaptive sampling under sustained overload** — applied at the **gateway** (global visibility), not the SDK: keep all ERROR/FATAL, sample WARN 50%, INFO/DEBUG 10%, tag kept entries with the sample rate. Tail-based sampling (keep everything from any errored request) is the fancier version; head-by-severity is usually enough.

---

## Real-World Anchor

**Datadog / CloudWatch Logs** are exactly this shape: a thin SDK with a bounded async buffer, an ingest tier that fronts a durable log (Kafka/Kinesis), an indexing fleet that materializes a search index, and hot/cold tiering to object storage. **Uber's logging pipeline** (per ByteByteGo) makes the same move — Kafka as the ingest shock absorber decoupling emit rate from indexing rate — and reaches for ClickHouse where the workload tilts toward metrics/aggregates rather than ad-hoc full-text, which is precisely the ES-vs-ClickHouse fork we drew above. The through-line: **treat the durable log as the source of truth and the search store as a rebuildable derived view** — replay from Kafka reconstructs ES after any indexing-tier loss. *(DDIA Ch. 11–12: this is stream processing building a derived, disposable index off an ordered log.)*

---

## 🔍 Senior-Signal Questions to Ask in Your Interview

- **"Is the search index authoritative, or is Kafka the source of truth and ES a rebuildable derived view?"** → *Why it matters: signals you understand the log-as-source-of-truth pattern — you can wipe and rebuild ES from Kafka, which reframes ES failures from data loss to recovery latency (DDIA Ch. 12).*
- **"What's our exactly-vs-at-least-once posture on indexing, and why is at-least-once acceptable here?"** → *Why it matters: shows you connect offset-commit-after-write to replay, and know that `log_id` idempotency makes duplicates harmless — a domain-specific reason at-least-once is the right, cheaper choice.*
- **"At what tenant size does `hash(tenant+service)` stop containing hot partitions, and what's the next lever?"** → *Why it matters: naming the exact inflection point (single tenant saturating even multi-service spread → round-robin or dedicated topic) is the senior tell, not reflexively dismissing the simple key.*
- **"Do we fail open or closed when the indexing pipeline is behind?"** → *Why it matters: forces the availability-during-incident trade — partial-results-with-caveat vs. erroring — which is the whole point of an observability tool.*
- **"How do we enforce per-tenant retention when tenants share a daily index?"** → *Why it matters: surfaces the query-time-filter vs. index-drop tension and shows you thought past the happy-path 'one delete call' retention story.*
- **"Where does sampling live — SDK or gateway — and why?"** → *Why it matters: the gateway has global per-tenant + pipeline-health visibility the SDK lacks; putting adaptive control where the information is is a systems-thinking signal.*