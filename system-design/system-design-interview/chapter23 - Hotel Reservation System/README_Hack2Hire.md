# Design a Hotel Booking System

> **Thesis:** *Search is fast but stale; the hold is where truth begins.* The system runs two consistency worlds over one shared inventory — an eventually-consistent search path over Elasticsearch/Redis, and a strongly-consistent reservation path over PostgreSQL. They meet at the **hold boundary**, where `SELECT FOR UPDATE` on date-range-scoped room-night rows guarantees exactly one winner per room-night.

---

## 1. Requirements

### Clarifying Questions (as dialogue)

| You ask | Interviewer says | Takeaway |
|---|---|---|
| Full hotel management platform, or just the guest-facing booking experience (search, hold, confirm, cancel)? | Just the guest flow. Onboarding, property mgmt, revenue mgmt out of scope. | Spend depth on **booking lifecycle + inventory correctness**, not hotel-side tooling. |
| If two guests race to confirm the last room for the same nights, must only one win — or can we resolve conflicts after? | Double-booking is **not acceptable**. Exactly one winner per room-night. | Need a **true single-winner guarantee**, not a "fix it later" model. |
| Must search availability be up-to-the-second, or is a few seconds of lag acceptable? | A few seconds of staleness in search is fine. The **hold** must catch conflicts. | Search and booking have **different consistency expectations**. |
| How long does a hold stay active if checkout is abandoned, and what happens to that inventory? | 10–15 min hold window. Unreleased holds must **auto-expire** back to inventory. | Hold expiry is a **product contract** — design must handle expiry + late payment explicitly. |

### Functional Requirements (prioritized)

1. **Search** available hotels by location, dates, guest count, filters (price, rating, amenities).
2. **Hold** a room type for a checkout window — temporarily reserving room-nights to prevent double-booking.
3. **Confirm** a held reservation via payment — transition `held → confirmed`.
4. *(Secondary)* **Cancel / modify** a confirmed booking subject to the hotel's policy.

### Non-Functional Requirements (quantified)

- **Consistency (split):** *Strong* on the reservation path — exactly one winner per room-night. *Eventual* on search (few-second staleness OK). **This split is the whole design.**
- **Latency:** Search < **500ms p95**; hold acquisition < **200ms p95**; confirmation < **1s p95** (includes gateway round-trip).
- **Durability:** All confirmed bookings + payment records durable and auditable. A crash must **never lose a confirmed reservation**.
- **Scale:** 50M listings, 500K bookings/day peak, 10M searches/day (~115 QPS avg, bursty), 5K concurrent booking attempts/min.
- **Integration boundary:** Payments via 3rd-party gateway (Stripe). We design the boundary, not the gateway.

### Capacity Estimation (only where it changes a decision)

I won't front-load QPS math. The two numbers that *move decisions*:

- **10M searches/day over 50M listings** → search **cannot** hit the authoritative booking DB. A derived index (Elasticsearch) + cache (Redis) is mandatory, not optional.
- **~3 room-nights/booking × 5K concurrent attempts/min** → each hold locks **multiple** inventory rows. **Atomicity of the multi-row hold is the real pressure point**, especially in flash sales. 500K bookings/day is otherwise trivial for a single Postgres primary.

---

## 2. Core Entities

- **Hotel** — property metadata (name, geo, star rating, amenities). Read-heavy, rarely mutated.
- **Room** — a room *type* per hotel (Standard Queen, Deluxe King) with capacity + base rate.
- **RoomNightInventory** — *the unit of scarcity.* One row = one room type × one hotel × one date, carrying `total_count` and `available_count`. **This is the row that gets locked.**
- **Booking** — the lifecycle owner: guest, hotel, room type, dates, `status` (held/confirmed/cancelled/completed/expired), `hold_expires_at`, `quoted_price`, payment ref.
- **BookingRoomNight** — junction: links a booking to each inventory row it claims (one per night). Makes release trivial.
- **Payment** — a charge attempt against a booking: gateway `charge_id`, amount, currency, status.

---

## 3. API / System Interface

Two-step commit — **hold first, then confirm with payment.** REST for mutations; webhooks for gateway callbacks; async queue for side-effects. User identity derived from the auth token, never the body.

```
GET    /v1/hotels/search?location=&check_in=&check_out=&guests=&filters=
         → best-effort availability from index/cache (may be stale)

GET    /v1/hotels/{hotel_id}
         → metadata, room types, photos, pricing for selected dates

POST   /v1/bookings/hold
         body: { hotel_id, room_type_id, check_in, check_out }
         → atomically decrements available_count per room-night;
           returns { booking_id, hold_token, ttl }  |  409 if unavailable

POST   /v1/bookings/{booking_id}/confirm
         body: { payment_token }
         → validates hold not expired, charges gateway, held → confirmed
           idempotent on booking_id (natural idempotency key)

DELETE /v1/bookings/{booking_id}     → cancel, apply policy, refund, release inventory
PATCH  /v1/bookings/{booking_id}     → modify dates/room type, re-check availability
GET    /v1/bookings/{booking_id}     → current state, dates, payment status, policy

POST   /v1/webhooks/payment          → async gateway confirmation; idempotent on booking_id
```

**The hold is the consistency boundary.** It only touches the booking DB (returns in <200ms). The confirm endpoint owns the slower gateway round-trip. Separating them gives the guest a responsive checkout without holding inventory for the full payment latency.

> **How I'd say it in the interview:** *"Two-step hold-then-confirm. The hold is fast — it only touches our DB and returns a token + TTL. The confirm is slower because it calls the payment gateway. Separating them means a responsive checkout without blocking inventory for the full payment round-trip."*

---

## 4. High-Level Design

### The core tension

Two workloads share one inventory. **Search:** 10M/day over 50M listings, needs <500ms, tolerates staleness. **Reservation:** 500K/day, must never double-book. Different consistency models, different storage engines, different latency profiles — but they must **agree on inventory at the moment the guest clicks "Book."**

### Why the naive single-DB design fails

One hotel service + one Postgres, serving both search and booking, breaks in three ways under real load:

1. **Concurrent holds race** — two guests both read "available," both commit, double-book.
2. **No timed expiry** — abandoned holds lock inventory forever; nothing reclaims it.
3. **Search does full table scans** — date-range availability over raw columns can't serve 10M queries/day at 500ms.

The fixes shape the architecture: a **transactional hold** (atomic multi-row lock), a **sweeper** (reclaim abandoned holds), and a **derived availability index** (search without touching the booking DB).

### Architecture


<svg viewBox="0 0 900 560" xmlns="http://www.w3.org/2000/svg" font-family="'Comic Sans MS','Segoe Print',cursive" font-size="13">
  <style>
    .box{fill:#fffef7;stroke:#2b2b2b;stroke-width:2;}
    .svc{fill:#eaf4ff;stroke:#26527a;stroke-width:2;}
    .store{fill:#fff3e0;stroke:#8a5a00;stroke-width:2;}
    .cache{fill:#f0ffe8;stroke:#3a6b1e;stroke-width:2;}
    .queue{fill:#fbe9ff;stroke:#7a2678;stroke-width:2;}
    .ext{fill:#f2f2f2;stroke:#555;stroke-width:2;stroke-dasharray:5 3;}
    .lbl{fill:#2b2b2b;}
    .solid{stroke:#2b2b2b;stroke-width:2;fill:none;marker-end:url(#ah);}
    .dash{stroke:#777;stroke-width:1.6;fill:none;stroke-dasharray:5 4;marker-end:url(#ahg);}
    .note{fill:#666;font-size:11px;}
  </style>
  <defs>
    <marker id="ah" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6" fill="#2b2b2b"/></marker>
    <marker id="ahg" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6" fill="#777"/></marker>
  </defs>

  <rect class="box" x="20" y="240" width="90" height="50" rx="6"/>
  <text class="lbl" x="65" y="270" text-anchor="middle">Guest</text>

  <rect class="svc" x="150" y="235" width="90" height="60" rx="6"/>
  <text class="lbl" x="195" y="260" text-anchor="middle">API</text>
  <text class="lbl" x="195" y="278" text-anchor="middle">Gateway</text>

  <rect class="svc" x="300" y="90" width="120" height="55" rx="6"/>
  <text class="lbl" x="360" y="114" text-anchor="middle">Search</text>
  <text class="lbl" x="360" y="132" text-anchor="middle">Service</text>

  <rect class="svc" x="300" y="380" width="120" height="55" rx="6"/>
  <text class="lbl" x="360" y="404" text-anchor="middle">Booking</text>
  <text class="lbl" x="360" y="422" text-anchor="middle">Service</text>

  <rect class="cache" x="500" y="60" width="110" height="48" rx="6"/>
  <text class="lbl" x="555" y="80" text-anchor="middle">Redis</text>
  <text class="note" x="555" y="97" text-anchor="middle">hot avail counts</text>

  <rect class="store" x="500" y="140" width="110" height="55" rx="4"/>
  <text class="lbl" x="555" y="163" text-anchor="middle">Elasticsearch</text>
  <text class="note" x="555" y="180" text-anchor="middle">avail index</text>

  <g>
    <path d="M495,390 v70 a60,14 0 0 0 120,0 v-70" fill="#fff3e0" stroke="#8a5a00" stroke-width="2"/>
    <ellipse cx="555" cy="390" rx="60" ry="14" fill="#fff3e0" stroke="#8a5a00" stroke-width="2"/>
    <text class="lbl" x="555" y="428" text-anchor="middle">PostgreSQL</text>
    <text class="note" x="555" y="446" text-anchor="middle">bookings·inventory·payments</text>
  </g>

  <rect class="queue" x="500" y="270" width="110" height="48" rx="6"/>
  <text class="lbl" x="555" y="290" text-anchor="middle">SQS</text>
  <text class="note" x="555" y="307" text-anchor="middle">side-effects</text>

  <rect class="svc" x="690" y="200" width="120" height="55" rx="6"/>
  <text class="lbl" x="750" y="224" text-anchor="middle">Availability</text>
  <text class="lbl" x="750" y="242" text-anchor="middle">Sync Worker</text>

  <rect class="svc" x="300" y="480" width="120" height="45" rx="6"/>
  <text class="lbl" x="360" y="507" text-anchor="middle">Expiry Sweeper</text>

  <rect class="ext" x="690" y="380" width="120" height="55" rx="6"/>
  <text class="lbl" x="750" y="404" text-anchor="middle">Payment</text>
  <text class="lbl" x="750" y="422" text-anchor="middle">Gateway (Stripe)</text>

  <path class="solid" d="M110,262 H150"/>
  <path class="solid" d="M240,255 C270,240 275,130 300,120"/>
  <text class="note" x="248" y="182">GET /search</text>
  <path class="solid" d="M240,275 C270,300 275,400 300,405"/>
  <text class="note" x="240" y="345">POST /hold /confirm</text>

  <path class="dash" d="M420,105 C460,92 470,86 500,84"/>
  <text class="note" x="428" y="80">read-through</text>
  <path class="dash" d="M420,125 C460,150 470,165 500,167"/>

  <path class="solid" d="M420,405 C450,400 465,398 495,395"/>
  <text class="note" x="425" y="392">SELECT FOR UPDATE</text>

  <path class="solid" d="M420,392 C450,360 470,320 500,300"/>
  <text class="note" x="432" y="348">emit event</text>

  <path class="solid" d="M610,285 C650,270 665,255 690,240"/>
  <path class="dash" d="M750,255 C740,320 660,370 615,392"/>
  <text class="note" x="668" y="332">read inventory</text>
  <path class="solid" d="M690,215 C660,190 622,175 612,172"/>
  <text class="note" x="614" y="205">update ES</text>
  <path class="solid" d="M700,205 C640,140 600,110 612,90"/>
  <text class="note" x="628" y="122">invalidate Redis</text>

  <path class="solid" d="M420,420 C520,450 620,432 690,415"/>
  <text class="note" x="500" y="462">charge (confirm)</text>

  <path class="solid" d="M420,495 C470,485 470,470 500,452"/>
  <text class="note" x="425" y="478">release expired holds</text>

  <text class="note" x="30" y="545">solid = correctness-critical path      ---- dashed = derived / read-path helper</text>
</svg>

The architecture splits into two stories.

**Story 1 — the authoritative reservation path.** On "Book," the Booking Service opens a Postgres transaction, `SELECT FOR UPDATE`s the room-night rows, checks `available_count > 0` for every night, decrements, inserts a `held` booking + junction rows, commits. Row-level locks guarantee exactly one winner. On confirm, it charges the gateway, flips to `confirmed`, and emits an SQS event for side-effects (email, cache invalidation, index update). **The Booking Service is the sole writer** — Postgres row locks are the only concurrency primitive needed. No distributed locks.

**Story 2 — the cheap availability path.** Search hits Elasticsearch (hotel metadata, pricing, per-date availability flags), fronted by Redis for hot hotels/near-term dates. When inventory changes, an SQS event drives the **Availability Sync Worker** to re-read Postgres and update ES + invalidate Redis. Search is allowed to be stale because the **hold catches conflicts** — a guest sees "unavailable" at hold time, not after entering card details.

**How they connect:** the paths meet at the inventory boundary. Reservation writes `available_count` in ACID transactions; search reads derived state that flows Postgres → SQS → worker → ES/Redis, lagging by a few seconds. This lets search scale (more ES replicas, more Redis) without touching reservation throughput.

> **How I'd say it in the interview:** *"Two consistency worlds. Search is fast but stale — ES + Redis. Booking is slow but correct — Postgres with row locks. They meet at the hold boundary, where the system transitions from best-effort availability to authoritative reservation."*

*DDIA Ch. 7 (Transactions) grounds the reservation path; Ch. 5 (Replication) and Ch. 11 (Stream Processing) ground the async Postgres→ES derivation.*

---

## 5. Deep Dives

### 5.1 Preventing double-booking under concurrent pressure

The correctness boundary of the whole system. The key insight: **room-night inventory is date-range-scoped scarcity, not a simple counter.** One physical room generates a separate bookable slot per night; a reservation must **atomically claim a contiguous range** of slots. A 3-night booking locks three rows — if any one hits zero, the entire hold fails.

The mechanism is a Postgres transaction with **ordered row-level locking**:

```sql
BEGIN;

SELECT id, available_count
FROM room_night_inventory
WHERE hotel_id = :hotel_id AND room_type_id = :room_type_id
  AND date BETWEEN :check_in AND :check_out - INTERVAL '1 day'
ORDER BY date            -- consistent ordering prevents deadlocks
FOR UPDATE;

-- app checks: every row has available_count > 0, else ROLLBACK

UPDATE room_night_inventory SET available_count = available_count - 1
WHERE hotel_id = :hotel_id AND room_type_id = :room_type_id
  AND date BETWEEN :check_in AND :check_out - INTERVAL '1 day';

INSERT INTO bookings (guest_id, hotel_id, room_type_id, check_in, check_out,
                      status, hold_expires_at, quoted_price)
VALUES (:g, :h, :rt, :ci, :co, 'held', now() + INTERVAL '15 minutes', :price);

INSERT INTO booking_room_nights (booking_id, room_night_inventory_id)
SELECT currval('bookings_id_seq'), id FROM room_night_inventory
WHERE hotel_id = :hotel_id AND room_type_id = :room_type_id
  AND date BETWEEN :check_in AND :check_out - INTERVAL '1 day';

COMMIT;
```

**Two guests race for the last room-night:** the first acquires the row locks and proceeds; the second **blocks** on `SELECT FOR UPDATE` until the first commits. By then `available_count = 0`, the app check fails, the hold is rejected. Lock hold time is bounded by the transaction (<100ms), so this doesn't starve the contended row.

**Ordering matters twice:** ordering by `date` gives every transaction the same lock-acquisition order, eliminating the ABBA deadlock where booking A locks night-1→night-2 while booking B locks night-2→night-1.

> **Senior-signal framing:** *SELECT FOR UPDATE gives us pessimistic serialization on exactly the contended rows without escalating to SERIALIZABLE isolation on the whole transaction. The alternative — optimistic concurrency via a `version` column + retry — wins under low contention but degrades in a flash sale where everyone fights the same room-night, because the retry storm amplifies the very contention it's trying to avoid. For a single hot room-night, pessimistic locking is the right default.*


---

### 5.2 Availability search across 50M listings

10M queries/day over 50M listings, each a date-range check ("Tokyo, Standard rooms, Mar 12–15"). Answering from Postgres in real time is infeasible — the system needs a **precomputed, slightly-stale availability view.**

The index lives in **Elasticsearch**: one document per room-type-at-hotel, carrying metadata, pricing, and a nested availability map keyed by date. On query, ES filters by location/dates/price/amenities, then a **script filter** verifies every requested night has a positive count — returning only room types with **full-range** availability.

Keeping it fresh is the challenge. On any inventory change (hold/confirm/cancel/expire), the Booking Service emits an SQS event; the Sync Worker re-reads Postgres for the affected room-type + date range, updates the ES doc, and invalidates Redis (short 30–60s TTL on hot counts).

**Staleness is bounded and self-correcting:** a guest who clicks "Book" on a stale listing gets a clean rejection at hold time — never a double-booking. **Sync lag** (booking-commit → ES-update delay) is the early-warning metric. If lag exceeds threshold, the Search Service **falls back to direct Postgres availability queries** for high-demand properties, trading latency for freshness until the pipeline recovers.

> **Senior-signal framing:** *This is a CQRS split — Postgres is the write model, ES is a materialized read model derived asynchronously. The correctness argument is that the read model is allowed to be wrong because the write model re-validates at the hold boundary. That inversion — "let search lie, let the hold tell the truth" — is what makes the whole thing cheap.*

---

### 5.3 Booking lifecycle & the hold-expiry race

The state machine looks trivial on paper — the edge cases define the system.

<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" font-family="'Comic Sans MS','Segoe Print',cursive" font-size="13">
  <style>
    .st{fill:#eaf4ff;stroke:#26527a;stroke-width:2;}
    .term{fill:#f0ffe8;stroke:#3a6b1e;stroke-width:2;}
    .bad{fill:#ffecec;stroke:#a12b2b;stroke-width:2;}
    .lbl{fill:#2b2b2b;}
    .edge{stroke:#2b2b2b;stroke-width:2;fill:none;marker-end:url(#a2);}
    .note{fill:#666;font-size:11px;}
  </style>
  <defs><marker id="a2" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6" fill="#2b2b2b"/></marker></defs>

  <ellipse class="st" cx="90" cy="140" rx="55" ry="30"/>
  <text class="lbl" x="90" y="145" text-anchor="middle">held</text>

  <ellipse class="st" cx="320" cy="70" rx="60" ry="30"/>
  <text class="lbl" x="320" y="75" text-anchor="middle">confirmed</text>

  <ellipse class="bad" cx="320" cy="220" rx="55" ry="30"/>
  <text class="lbl" x="320" y="225" text-anchor="middle">expired</text>

  <ellipse class="term" cx="560" cy="70" rx="60" ry="30"/>
  <text class="lbl" x="560" y="75" text-anchor="middle">completed</text>

  <ellipse class="term" cx="560" cy="220" rx="60" ry="30"/>
  <text class="lbl" x="560" y="225" text-anchor="middle">cancelled</text>

  <path class="edge" d="M135,120 C200,90 240,80 262,74"/>
  <text class="note" x="150" y="92">confirm (pay ok)</text>
  <path class="edge" d="M135,160 C200,190 240,205 267,214"/>
  <text class="note" x="150" y="205">TTL fires</text>
  <path class="edge" d="M380,60 C440,50 470,55 500,63"/>
  <text class="note" x="400" y="42">check-out (batch)</text>
  <path class="edge" d="M380,80 C440,120 470,160 505,205"/>
  <text class="note" x="452" y="150">cancel + policy</text>

  <text class="note" x="90" y="215" text-anchor="middle">hold_expires_at</text>
  <text class="note" x="90" y="230" text-anchor="middle">= now()+15min</text>
</svg>

The tricky case is a **race between hold expiry and payment.** Guest submits payment at minute 14, the gateway takes 90s, the hold expires at minute 15 — the sweeper releases inventory while payment is in flight. Gateway then returns *success*, but the booking is already `expired`. Sequence:

<svg viewBox="0 0 780 360" xmlns="http://www.w3.org/2000/svg" font-family="'Comic Sans MS','Segoe Print',cursive" font-size="12.5">
  <style>
    .life{stroke:#999;stroke-width:1.5;stroke-dasharray:4 4;}
    .head{fill:#eaf4ff;stroke:#26527a;stroke-width:2;}
    .msg{stroke:#2b2b2b;stroke-width:1.8;fill:none;marker-end:url(#a3);}
    .ret{stroke:#777;stroke-width:1.6;fill:none;stroke-dasharray:5 4;marker-end:url(#a3g);}
    .lbl{fill:#2b2b2b;}
    .note{fill:#666;font-size:11px;}
    .actbox{fill:#fff3e0;stroke:#8a5a00;stroke-width:1.5;}
  </style>
  <defs>
    <marker id="a3" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6" fill="#2b2b2b"/></marker>
    <marker id="a3g" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6" fill="#777"/></marker>
  </defs>

  <!-- lifelines -->
  <g>
    <rect class="head" x="30"  y="20" width="110" height="34" rx="5"/><text class="lbl" x="85"  y="42" text-anchor="middle">Booking Svc</text>
    <rect class="head" x="240" y="20" width="110" height="34" rx="5"/><text class="lbl" x="295" y="42" text-anchor="middle">PostgreSQL</text>
    <rect class="head" x="450" y="20" width="110" height="34" rx="5"/><text class="lbl" x="505" y="42" text-anchor="middle">Gateway</text>
    <rect class="head" x="640" y="20" width="110" height="34" rx="5"/><text class="lbl" x="695" y="42" text-anchor="middle">Sweeper</text>
    <line class="life" x1="85"  y1="54" x2="85"  y2="345"/>
    <line class="life" x1="295" y1="54" x2="295" y2="345"/>
    <line class="life" x1="505" y1="54" x2="505" y2="345"/>
    <line class="life" x1="695" y1="54" x2="695" y2="345"/>
  </g>

  <path class="msg" d="M85,80 H505"/>
  <text class="note" x="150" y="74">charge(payment_token)  [min 14]</text>

  <path class="msg" d="M695,120 H300"/>
  <text class="note" x="470" y="114">TTL+grace elapsed → try release</text>
  <rect class="actbox" x="278" y="130" width="34" height="30"/>
  <text class="note" x="330" y="150">expire only if still 'held'</text>

  <path class="ret" d="M505,200 H90"/>
  <text class="note" x="150" y="194">success  [min 15.5, late]</text>

  <path class="msg" d="M85,235 H295"/>
  <text class="note" x="120" y="229">BEGIN; SELECT status FOR UPDATE</text>

  <rect class="actbox" x="278" y="245" width="34" height="34"/>
  <path class="ret" d="M295,300 H90"/>
  <text class="note" x="120" y="294">status = 'expired'  → reject confirm</text>

  <path class="msg" d="M85,335 H505"/>
  <text class="note" x="140" y="329">refund(charge_id)  — compensating action</text>
</svg>

The defense is **two-part**:

1. **Re-validate under lock at confirm time.** Before accepting the gateway result, the Booking Service re-reads the booking `FOR UPDATE` and checks it's still `held`. If the sweeper already moved it to `expired`, the confirm path **rejects the late payment and issues a refund** (a compensating transaction — the money moved, so we un-move it). The DB is the arbiter, not the gateway's timing.
2. **Grace period on the sweeper.** It only expires holds past `TTL + buffer` (e.g. +2 min), giving in-flight payments time to land — shrinking the race window without holding inventory indefinitely.

Other transitions: **cancel** → apply policy, async refund, release inventory. **complete** → a daily batch scans bookings with `check_out` in the past and marks them `completed` for reporting/reconciliation.

> **How I'd say it in the interview:** *"Five states, but the interesting part is the hold-expiry race. If payment lands after expiry, the DB — not the gateway — decides: re-validate `held` under a row lock, and if it's already expired, reject and refund. A grace period on the sweeper narrows the window without locking inventory forever."*

*This is the classic saga compensation pattern — DDIA Ch. 9 (Consistency & Consensus) on why an external side-effect (the charge) needs a compensating action rather than a rollback, since you can't roll back a third party.*

---

## 6. Other Considerations

**Overbooking as deliberate strategy.** Hotels overbook by a margin because some confirmed guests no-show. Support it by letting `available_count` go slightly negative, bounded by a per-hotel threshold: the hold check becomes `available_count > -overbooking_limit` instead of `> 0`. Oversell recovery is a front-desk concern (upgrade, compensate, walk to a partner hotel); the system just **flags overbooked reservations on ops dashboards** so staff act before arrival.

**Cancellation policy & refund timing.** Policies (free until N days out → partial → non-refundable) are structured rules on the room type. On cancel, the service evaluates the policy against the **server's current time**, computes the refund, transitions to `cancelled`, and refunds asynchronously. It **records the applied policy + refund amount on the booking row** for audit — critical at the policy boundary.

**Rate consistency.** The **hold captures the quoted price** (`quoted_price` on the booking). Confirm charges the quoted rate, not the current rate — search price is approximate, hold price is authoritative. Rates propagate through the same async ES/Redis pipeline as availability.

**Monitoring — domain metrics, not just infra:**
- **Hold success rate** — a drop signals inventory exhaustion, contention spikes, or sync bugs.
- **Double-booking incidents** — must be **zero**; a reconciliation job scans for overlapping confirmed bookings on the same room-night. Any non-zero count is a P0.
- **Hold-to-confirm conversion** — low rate → TTL too short, checkout too slow, or pricing surprises.
- **Search latency p50/p95/p99** — degradation traces to ES health or cache miss rate.
- **Availability sync lag** — sustained lag → spike in failed holds. The leading indicator.

Reservation-path SLO: **99.9%**. Search can tolerate brief degradation; sustained search downtime hits revenue directly.

---

## Real-World Anchor

**Booking.com / Airbnb-style inventory** systems converge on exactly this CQRS split: a strongly-consistent transactional store of record for reservations, and an asynchronously-derived search index (Elasticsearch/Solr) that's allowed to lag because the booking step re-validates. **Bytebytego's ticket/reservation case studies** (Ticketmaster, movie-seat booking) use the same "reserve-then-confirm with TTL" two-phase pattern — the seat hold and the room-night hold are the same primitive: a short-lived, auto-expiring claim backed by a row-level lock, decoupling the fast reservation from the slow payment. The distinguishing move here versus a simple seat-booking system is **date-range-scoped scarcity** — a stay claims a *contiguous set* of inventory rows atomically, not a single seat.

---

## DDIA Chapter References

- **Ch. 7 (Transactions)** — `SELECT FOR UPDATE`, isolation levels, why pessimistic locking on the contended room-night beats optimistic retry under a flash sale; ordered lock acquisition to avoid deadlock.
- **Ch. 9 (Consistency & Consensus)** — the hold-expiry race and compensating refund; you can't roll back a third-party charge, so you compensate.
- **Ch. 5 (Replication)** + **Ch. 11 (Stream Processing)** — the Postgres → SQS → ES/Redis derivation pipeline and its bounded-staleness read model (CQRS).
- **Ch. 12 (The Future of Data Systems)** — deriving materialized search views from a system of record; "let the read model be wrong, let the write model be right."

---

## 🔍 Senior-Signal Questions to Ask in Your Interview

- **Pessimistic (`SELECT FOR UPDATE`) vs. optimistic (`version` + retry) concurrency for the hold — where's the crossover?** → *Why it matters: signals you understand contention regimes. Optimistic wins under low contention; pessimistic wins on a single hot room-night in a flash sale where retry storms self-amplify.*
- **What happens if the SQS side-effect for cache invalidation is lost or delayed?** → *Why it matters: probes back-pressure and the failure mode of the derived read model — the answer (hold re-validates, so staleness is safe) proves you understand why the async pipeline can fail without corrupting correctness.*
- **How do we tune the sweeper grace period against gateway p99 latency?** → *Why it matters: shows you connect an operational constant to a real distribution — too short leaks the race, too long over-holds inventory during flash sales.*
- **Single Postgres primary is the reservation bottleneck — what's the sharding key when we outgrow it?** → *Why it matters: `hotel_id` is the natural shard key (a booking never spans hotels), keeping every hold transaction single-shard and avoiding distributed transactions. Signals you know the scaling inflection point before it's a fire.*
- **The reconciliation job finds an overlapping double-booking — what's the runbook?** → *Why it matters: senior engineers plan for their invariant being violated. Detection + compensation (re-accommodate, comp, walk) matters as much as prevention.*
- **Why not Redis-based distributed locks (Redlock) for the hold instead of Postgres row locks?** → *Why it matters: Redlock gives mutual exclusion without a fencing token, and a double-grant here means a corrupted double-booking — so the resource with native compare-and-set (the DB row) is strictly safer. Shows you know when a distributed lock is disqualified.*