
# Design Whatsapp

## 1. Requirements (~5 min)

![data-tables](images/hello-interview/1.png)
---

## 3. Core Entities

1. Users: UserId, Metadata
2. Chats (2-100 users): ChatId, Name, Metadata
3. ChatParticipient : ChatId (PK), ParticipientId (GSI)
4. Messages: MessageId, chatId, Contents, UserId, Timestamp
5. Clients (a user might have multiple devices): DeviceID, USerId, IP etc
6. Inbox (Undelivered Messages): UserId, MessageId

## 2. API Design

![data-tables](images/hello-interview/2.png)

## High-Level Design (~10–15 min)

### Arpit 
![data-tables](images/arpit/1.png)
![data-tables](images/arpit/2.png)

### ShowOffer
![data-tables](images/show-offer/1.png)
![data-tables](images/show-offer/2.png)
![data-tables](images/show-offer/3.png)


![data-tables](images/hello-interview/3.png)
---

## Deep Dives (~10 min)

### WhatsApp / Chat — Last-Minute Deep Dive Notes

> **One-line thesis:** *Durability lives in the DB (Inbox/Message tables), real-time delivery is best-effort (Pub/Sub) — everything reconciles against the Inbox on reconnect.*

---

### 1. Scaling to billions of users (routing / "host confusion")

**Problem:** Single host can't serve ~200M concurrent connections. Scale out Chat Servers → but sender and recipient may land on *different* servers, so a server holding a message may not hold the recipient's connection.

**Options (rejected → chosen):**

| Approach | Verdict | Why |
|---|---|---|
| LB + horizontal scale, nothing else | ❌ Broken | No guarantee the server has the recipient's connection. Can't deliver. |
| Kafka topic per user | ❌ Broken | Kafka not built for billions of topics (~50KB overhead each → 50TB+ for 1B users). "Super topics" just reinvent worse versions of the options below. |
| Consistent-hash users → specific Chat Server (ZooKeeper/Etcd registry) | ⚠️ Good | Deterministic ownership, direct delivery. But every server must connect to every other server → **big servers, small count**. Scaling requires careful connection-draining to avoid thundering herd; dual-publish during rebalancing to avoid dropped messages. |
| **Redis Pub/Sub** (channel per userID) | ✅ Chosen | Lightweight in-memory hashmap of socket pointers, no persistence. Shard channels across a Redis cluster by userID. |

**Redis Pub/Sub write path (order matters):**
1. Write to Message table + create Inbox entries **(durable)**
2. Return success to sender
3. Publish to Pub/Sub for real-time delivery **(best-effort)**

→ If step 3 fails, message is safe; recipient gets it on reconnect (Inbox sync) or via polling.

**Key facts to drop:**
- Pub/Sub is **at-most-once** — no subscriber / transient failure = message lost. Acceptable *because* of the durable write-first ordering.
- Scalability is a non-issue: Canva benchmark = 100K updates/sec on one Redis host at 27% util. Pub/Sub is "dumb," just forwarding.
- Cost: extra single-digit-ms latency (ferrying via Redis) + N connections per Chat Server ↔ Redis cluster (small, surmountable).

**Partition by user or by chat?**
- Depends on (a) chats per user, (b) chat size.
- WhatsApp = mostly 1:1 + hard cap of 100 participants → **partition by user**. Per-chat channels create redundant subscriptions for little gain.
- **Senior edge case (celebrity problem):** large chats are the rare case disproportionately stressing the system. Fix = *adaptive partitioning*: for chats > threshold (~25 users), subscribe to a per-chat channel too; publish to chat-channel when chat is large. Watch the transition — dual-publish briefly so servers have time to subscribe.

---

### 2. Multiple clients per user (phone + laptop + tablet)

Can't rely on a per-*user* Inbox anymore (laptop was off, must catch up independently).

**Changes:**
- New **Clients table** keyed by userID → resolve user to 1..N active clients.
- Inbox becomes **per-client**, not per-user.
- Chat participant lookup expands to all clients of each user.
- Send → deliver to all clients of the user.
- **Pub/Sub unchanged** — still subscribe by userID.
- Need **deactivation** of dead clients (stop storing for them) + a **limit (~3 clients/account)** to cap storage/throughput.

---

### 3. WebSocket connection failure (silent death)

TCP keepalives take *minutes* — far too slow. Socket can look "open" but be functionally dead.

**Layered approach:**

1. **Do nothing** ❌ — TCP eventually times out; user misses messages meanwhile. Unacceptable.
2. **ACK + retry on send** — server waits for client ACK (500–2000ms), retries, then closes socket after a few failures → forces reconnect + Inbox sync. Reuses the existing Inbox-clearing ACK. *Gap: only detects failure when actively sending.*
3. **Heartbeats (chosen backstop)** — server pings every 10–30s; client must pong within ~5s or socket closes.
   - Detects dead connections in **seconds, not minutes**.
   - Guaranteed upper bound: interval + timeout (e.g. 10s + 5s = detect within 15s).
   - Overhead: 200M users × 10s interval = ~20M ping/pong per sec — fine, tiny messages.

---

### 4. Redis dropped a message (at-most-once) — fast recovery for *connected* clients

Durability already handled (Inbox write-first ⇒ eventual delivery). Question is how connected clients quickly notice a drop.

1. **Periodic poll / sync** — client sends sync every 30–60s, server checks Inbox. *Cost:* 200M users / 30s ≈ **7M QPS** just for syncs. Tunable knob (latency ↔ load). "Good enough."
2. **Per-chat sequence numbers** — monotonic seq per chat via Redis `INCR`. Client sees #5 but last saw #3 → knows #4 missing → re-sync. *Gap: only detects on receipt; quiet chat hides the gap.*
3. **Heartbeat + global per-user sequence (chosen combo):**
   - Single incrementing counter per user; every message to them increments it.
   - Server piggybacks current seq on the heartbeat ping.
   - Client's local seq behind server's → immediate sync request.
   - Fast detection (within one heartbeat), near-zero extra load (heartbeat already exists).
   - Cost: atomic counter (Redis `INCR`) = extra coordination/dependency.

**Production reality = all three:** heartbeats detect dead sockets, sequence numbers detect missed messages, polling is the final backstop.

---

### 5. Out-of-order messages

**We don't reorder.** Guaranteeing send-order is expensive (buffering delays + reordering, cf. Flink bounded-out-of-orderness watermarks) and users prefer *fast* over *ordered*.

- Chat Servers sync clocks via **NTP** (good, not perfect).
- Server **stamps message on receipt**; clients display ordered by that server timestamp → consistent ordering across all clients.
- Occasionally a message "pops in above" a later one. **Users find this acceptable.**

---

### 6. "Last seen"

**Naive:** update `lastSeen` on every action (send/receive/heartbeat) ❌ — 200M users × 10–30s heartbeats = millions of writes/sec of near-worthless, instantly-stale data. Even DynamoDB strains; paying for strong consistency we don't need.

**Chosen — write only on disconnect + ask if online:**
- Two insights: (1) we *know* when a WebSocket connects/disconnects; (2) if a user is online they can answer for themselves.
- **LastSeen table (DynamoDB):** 1 record/user, updated only on disconnect. Use **conditional expression** (`only update if new ts > existing`) so racing servers don't overwrite a more-recent disconnect.
- Messages: `getLastSeen {targetUserId, requestingUserId}` and `updateLastSeen {targetUserId, reporter: DATABASE|SERVER, lastSeen: ONLINE|$DATE}`.

**Flow:**
1. Client publishes `getLastSeen`.
2. Chat Server, in parallel:
   - **2a** reads LastSeen table → publishes `updateLastSeen` ($DATE) to requester's channel.
   - **2b** forwards `getLastSeen` to target's channel.
3. If target is connected, its Chat Server publishes `updateLastSeen: ONLINE` to requester.
4. Client merges: got ONLINE → green bubble; else show last disconnect time.

**Trade-offs:** possible delay between the two responses → client waits briefly or updates UI seamlessly. Depends on servers reporting disconnects — if a server dies, users reconnect shortly; for robustness, also write LastSeen **on connect**.

---

## 🔍 Senior-Signal Questions to Ask in Your Interview

- **"Should Pub/Sub channels be keyed by user or by chat, and would you change that adaptively?"** → *Why it matters: shows you spotted the celebrity/hot-key problem and know partitioning strategy is workload-dependent, not fixed.*
- **"What's our ordering guarantee — and are we willing to trade it for latency?"** → *Why it matters: signals you understand distributed ordering is expensive and that product UX (fast > perfectly ordered) drives the CAP-ish call.*
- **"Where exactly does durability live vs. real-time delivery?"** → *Why it matters: the write-first (DB) then publish (best-effort) ordering is the core correctness invariant; getting the order wrong loses messages.*
- **"How do we detect a silently dead WebSocket, and what's the bounded detection time?"** → *Why it matters: recognizing TCP keepalives are minutes-slow and quantifying heartbeat interval+timeout is a concrete failure-mode answer.*
- **"How do we avoid a thundering herd / dropped messages when scaling Chat Servers up or down?"** → *Why it matters: connection draining + dual-publish during rebalance is the operational nuance that separates senior from mid-level.*

---

## DDIA Anchors
- **Ch. 5 (Replication)** — DynamoDB LastSeen conditional writes = leaderless conflict avoidance; write-first-then-publish is a replication-ordering choice.
- **Ch. 8/9 (Distributed trouble / Consistency & Consensus)** — NTP clocks are unreliable but "good enough"; why we *don't* attempt total order. ZooKeeper/Etcd for consistent-hash membership = consensus for coordination.
- **Ch. 11 (Stream Processing)** — Kafka topic-per-user rejection; Flink watermarks as the "if we *did* reorder" reference.

## Real-World Anchor
Discord (Bytebytego) runs the same shape: Elixir/Erlang session servers hold WebSocket state, a lightweight fanout layer routes events between session nodes, and durable message history lives in Cassandra — real-time is best-effort over a durable log, exactly the Inbox + Pub/Sub split above.
## Best Design