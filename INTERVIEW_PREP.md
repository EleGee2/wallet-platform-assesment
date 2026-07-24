# Interview Prep: Wallet Platform Assessment

A study guide covering the codebase, every fix made, and the reasoning
behind each decision — organized as questions you could plausibly get
asked, with model answers grounded in the actual code. Not a deliverable;
just for your own prep.

## How to use this

Skim the table of contents, then focus on sections for work you're least
confident explaining cold. The "core mechanics" section (idempotency,
transactions, outbox/inbox) is the highest-leverage material — it's the
pattern repeated across almost every fix, so understanding it once lets
you answer variations of the same question about five different files.

## Contents

1. [30-second project summary](#1-30-second-project-summary)
2. [Architecture walkthrough](#2-architecture-walkthrough)
3. [Core mechanics you'll be asked to explain](#3-core-mechanics-youll-be-asked-to-explain)
4. [The P0/P1/P2 fixes, one by one](#4-the-p0p1p2-fixes-one-by-one)
5. [The six bonus features](#5-the-six-bonus-features)
6. [The four bonus features you deliberately skipped](#6-the-four-bonus-features-you-deliberately-skipped)
7. [Testing strategy](#7-testing-strategy)
8. [Trade-offs and "why not X instead"](#8-trade-offs-and-why-not-x-instead)
9. [What's still broken / remaining debt](#9-whats-still-broken--remaining-debt)
10. [Likely curveball questions](#10-likely-curveball-questions)
11. [Glossary — rapid-fire definitions](#11-glossary--rapid-fire-definitions)

---

## 1. 30-second project summary

**"Walk me through what this project is."**

A NestJS wallet/ledger service: users hold wallets, deposit, withdraw, and
transfer money between wallets. MongoDB is the source of truth, Redis
caches wallet balances for fast reads, RabbitMQ carries the `transfer`
flow's async settlement (debit the sender synchronously, credit the
receiver asynchronously once the event is consumed). Every money movement
is recorded as a double-entry ledger line (debit + credit), independent of
the wallet's own denormalized balance field.

It was handed to me as a "working but flawed" codebase — the brief was to
find and fix the highest-impact correctness/reliability issues under a
time budget, not rewrite everything. I prioritized: (1) an actual balance
race that could go negative under load, (2) an unfinished transfer feature
with no idempotency and no exactly-once delivery, then a descending list
of cheaper, still-real issues, then a set of "bonus" architecture patterns
(outbox, inbox, DLQ, rate limiting, reconciliation, audit) layered on top
once the core was solid.

---

## 2. Architecture walkthrough

**"Walk me through what happens when someone calls `POST /wallets/transfer`."**

1. Request hits `WalletsController.transfer()`, validated by
   `TransferDto`, rate-limited by a per-route `@Throttle` override.
2. `WalletsService.transfer()` opens a MongoDB session and
   `session.withTransaction(...)`:
   - Checks `idempotencyKey` first (fast path: if this exact key was
     already used, return the original `Transfer` document instead of
     doing anything else).
   - Atomically debits the sender via `findOneAndUpdate` with a
     `balance: { $gte: amount }` guard and `$inc: { balance: -amount }` —
     one round trip, no read-then-write.
   - Creates the `Transaction` + `LedgerEntry` (debit leg).
   - Creates the `Transfer` document (`status: PENDING`).
   - Stages a `transfer.initiated` event via `OutboxService.enqueue()` —
     **not** a direct RabbitMQ publish (see §3, outbox pattern).
   - Commits. If any step fails, everything rolls back together.
3. After the transaction commits (not inside it), invalidates the sender's
   Redis balance cache.
4. Response returns immediately with the `Transfer` in `PENDING` — the
   receiver isn't credited yet.
5. Separately, `OutboxRelayWorker` (a `setInterval` loop) polls for
   `PENDING` outbox events and publishes them to RabbitMQ via
   `RabbitMQService.publish()`, then marks them `PUBLISHED`.
6. `TransferEventsConsumer` (subscribed to `transfer.events.queue`) picks
   up the `transfer.initiated` message, and inside its own transaction:
   claims an inbox `messageId` (dedup), atomically flips the `Transfer`
   from `PENDING` → `COMPLETED` (guarded, so redelivery is a no-op),
   credits the receiver wallet, writes the credit `Transaction`/
   `LedgerEntry`, and invalidates the receiver's cache.
7. If that consumer step fails, it's nacked-and-requeued once; a second
   failure routes it to a dead-letter queue instead of dropping it.
8. Separately, `PendingTransferWorker` sweeps for transfers stuck
   `PENDING` past a timeout and re-stages them via the outbox — a
   self-healing path for the case where step 5/6 never happened (lost
   event, consumer was down, etc).

**"What are the module boundaries?"**

- `wallets/` — the money-movement API surface (deposit/withdraw/transfer/
  dashboard/reconcile/audit) and its schemas.
- `transactions/` — read-only transaction history query endpoint.
- `ledger/` — double-entry ledger records + the service that writes/reads
  them (`recordDebit`/`recordCredit`/`computeBalanceFromLedger`/
  `aggregateNetByWallet`).
- `outbox/` — the transactional outbox schema/service.
- `queue/` — `RabbitMQService` (publisher) and `TransferEventsConsumer`.
- `workers/` — the three interval-driven background workers.
- `redis/` — the balance cache wrapper, fail-open by design.
- `auth/` — JWT login/guard.
- `common/` — cross-cutting concerns: correlation-id middleware, logging
  interceptor, exception filter, the `@Public()` decorator.
- `config/` — typed config loader reading from env vars.

**"Why does MongoDB need to run as a replica set here?"**

Multi-document ACID transactions (`session.withTransaction`) are only
supported against a replica set (even a single-node one) or a mongos
router — a standalone `mongod` rejects them outright. Every write path
that touches more than one collection (balance + transaction + ledger, at
minimum) needs this.

---

## 3. Core mechanics you'll be asked to explain

This is the highest-value section — almost every fix in this project is
one of these four patterns applied to a different call site.

### 3.1 Atomic updates instead of read-modify-write

**The bug pattern, everywhere it appeared:** `const wallet = await
Model.findById(id); wallet.balance -= amount; await wallet.save();` Two
concurrent requests both read the same starting balance, both pass a
JS-level check, both save — one write silently clobbers the other. No
error, no exception — just a lost update. This is the literal mechanism
behind "wallet balances have occasionally gone negative under load."

**The fix, everywhere:** one atomic command —

```js
walletModel.findOneAndUpdate(
  { _id: id, balance: { $gte: amount } },
  { $inc: { balance: -amount } },
  { new: true, session },
)
```

The `$gte` guard and the `$inc` happen as one indivisible operation at the
database level. If two requests race, MongoDB serializes them — the
second one's guard either still passes (if there was enough balance for
both) or fails cleanly (returns `null`, converted to a 400), instead of
silently overwriting. Deposits don't need the `$gte` guard (no lower
bound), just the atomic `$inc`.

**Why not optimistic locking (a `version` field + retry loop) instead?**
Both approaches close the same race, but the atomic-update approach
doesn't need a retry loop at all — the guard and mutation are one round
trip, versioning needs "read version, write, check version, retry on
mismatch." For a single-field balance mutation the atomic operator is
strictly simpler with the same guarantee, which is also why `Wallet`'s
existing (unused) `version` field was never wired up — there's no
read-modify-write call site left that would benefit from it.

### 3.2 Idempotency keys (pre-check + DB-level backstop)

Every mutating endpoint that accepts a caller-supplied key
(`WithdrawDto.reference`, `TransferDto.idempotencyKey`) follows the same
two-layer pattern:

1. **Fast pre-check**: before doing any real work, query for an existing
   document with that key. If found, short-circuit (return the existing
   result, or reject, depending on the endpoint — see below).
2. **DB-level backstop**: the key column has a **unique + sparse** index.
   Sparse means documents that don't supply a key at all are unaffected
   (unlimited normal transactions); unique means two documents *with* the
   same key can never both exist. A concurrent request that slips past
   the pre-check (classic TOCTOU — both requests check "does it exist?"
   before either has written yet) still collides at the index, throwing a
   MongoDB duplicate-key error (E11000), which the code catches and
   converts into a clean domain result instead of a raw 500.

**Why two layers instead of just the index?** The index alone is
correct but wasteful — a genuine retry would always do the full debit
attempt, hit the index, and get an error, when the pre-check can short
circuit before ever touching the balance. The pre-check is the fast path;
the index is the correctness guarantee under real concurrency.

**Why does `withdraw` return a 409 on replay but `transfer` returns the
*original transfer* on replay?** Read the DTOs' own doc comments —
`TransferDto.idempotencyKey`'s docstring explicitly says "retried requests
should reuse the same key" (implying replay should look like success,
returning what already happened), while `WithdrawDto.reference` doesn't
promise that. Different declared contracts, different handling — not an
inconsistency, a deliberate distinction.

### 3.3 The outbox pattern

**The problem it solves:** if you debit a wallet inside a MongoDB
transaction and then call `rabbitMQService.publish(...)` as the last line
*inside that same transaction*, you get a distributed-transaction problem
you can't actually solve: either (a) the publish succeeds but the Mongo
commit later fails/rolls back — now a durable message exists for a debit
that never happened — or (b) worse, `session.withTransaction` **retries
its entire callback function** on a `TransientTransactionError` (a normal,
expected occurrence under write conflicts) — so a direct publish call
inside that callback can fire **twice** for what the caller sees as one
request.

**The fix:** instead of publishing directly, write a row to an `outbox_events`
collection (`OutboxService.enqueue(routingKey, payload, session)`) as
*just another write inside the same transaction*. It commits or rolls back
atomically with the debit — no more, no less. A separate, idempotent
`OutboxRelayWorker` polls for pending outbox rows on an interval, actually
publishes them to RabbitMQ, and marks them `PUBLISHED`. This decouples
"did the business transaction commit" from "did the network call to
RabbitMQ succeed" — the two can never disagree.

**Follow-up they might ask: "Isn't the outbox relay itself an
at-least-once problem?"** Yes — if the worker crashes between
`publish()` succeeding and `markPublished()` running, that event gets
republished next tick. This is explicitly named as still-open in the
design doc. It's *acceptable* specifically because the consumer on the
other end is idempotent (see the inbox pattern, §3.4) — a duplicate
publish becomes a safe no-op downstream, not a duplicate credit.

**Where else this pattern applies:** `createWallet()`'s `wallet.created`
event, and — as a bonus fix — `PendingTransferWorker.sweep()`'s republish
(originally called RabbitMQ directly; fixed to route through the outbox
so a crash between marking `lastSweptAt` and publishing can't silently
lose the republish).

### 3.4 The inbox pattern (and why it's different from the status guard)

Two *different* idempotency mechanisms exist in the consumer, solving two
different problems:

1. **The transfer's own status guard** (`findOneAndUpdate({ _id, status:
   PENDING }, { status: COMPLETED })`) — this is *business-level*
   idempotency: "has this specific `Transfer` already been completed?"
   It was already fully correct on its own before the inbox pattern was
   added — redelivery of the *same* transfer, or two different messages
   both referencing the same transfer, are both safe no-ops because of
   this guard alone.
2. **The inbox pattern** (`inbox_messages` collection, keyed on a
   `messageId` minted fresh by `RabbitMQService.publish()` on every real
   network publish) — this is *message-level* idempotency: "has this
   exact AMQP delivery already been processed?", independent of what
   business entity it's about.

**So why add #2 if #1 already works?** Be honest about this if asked:
it's **defense-in-depth on an already-closed gap**, not a fix for a live
bug — explicitly called out as such in the design doc. Its actual value:
it's a *generic* mechanism that would still protect a future consumer that
doesn't have a natural status field to guard on, and it's an explicit
demonstration of a named architecture pattern (the README asked for it as
a bonus item).

**Why is the claim placed *first*, inside the transaction, rather than
before it as a separate pre-check-only step?** Two reasons: (a) a cheap
pre-check *does* run before opening a transaction at all (skip a wasted
attempt on the common repeat-delivery case), but (b) the *real* claim
(the `create()` call) has to be inside the same transaction as the rest of
completion — so that if the transaction later fails for an unrelated
reason (e.g. destination wallet not found), the claim rolls back
**together** with everything else. If the claim were separately committed
outside the transaction, a genuinely failed attempt would permanently
"poison" that `messageId`, and a legitimate redelivery retry would be
wrongly treated as already-handled.

**Why mint a fresh `messageId` on every `publish()` call instead of
reusing one?** Because `messageId` is meant to mark "this specific
delivery attempt," not "this business entity." A real broker redelivery
of the same in-flight message reuses the same AMQP properties (same
`messageId`) — that's the case the inbox is supposed to catch. A
genuinely *new* publish call (the sweep worker's republish, a retried
outbox relay) is legitimately a new delivery attempt and should get a new
id — it's not "the same message showing up twice," it's "we decided to try
again."

### 3.5 `AsyncLocalStorage` for correlation IDs

**The problem:** you want one id, generated at the HTTP layer, to show up
in every log line caused by that request — including in a completely
separate process/tick (the async consumer, hours later). Passing it as an
explicit parameter through every function signature in the call graph
would be extremely invasive.

**The fix:** a single shared `AsyncLocalStorage<{ correlationId }>`
instance (`request-context.ts`). `CorrelationIdMiddleware` wraps the rest
of the request pipeline in `requestContext.run({ correlationId }, next)` —
Node propagates that context across the entire subsequent await chain
automatically. Anything downstream just calls `getCorrelationId()` with no
awareness of who set it or how. The interceptor, the exception filter, the
outbox event, the AMQP message's native `correlationId` property, and the
consumer all read/write through this one primitive.

**Why does the sweep worker need `requestContext.run(...)` even though
it's not inside a request?** Because it's *reconstructing* a specific,
already-known correlation id (the one stored on the `Transfer` document
from when it was originally created) and needs `OutboxService.enqueue()`'s
ambient read of `getCorrelationId()` to pick it up — so it wraps that one
call in a freshly-established context carrying the id it already knows,
rather than threading it through as an explicit parameter.

---

## 4. The P0/P1/P2 fixes, one by one

Quick-reference — the mechanism (§3) is the interesting part; this is
"which file, which symptom."

| Issue | Where | Fix | Named production symptom |
|---|---|---|---|
| Withdraw race | `WalletsService.withdraw` | Atomic `$gte`/`$inc` (§3.1) | Balances going negative under load |
| Transfer idempotency + exactly-once | `WalletsService.transfer`, `TransferEventsConsumer` | Idempotency key (§3.2), status guard, outbox (§3.3) | Duplicate side effects, stuck transfers, unfinished feature |
| Sweep could flood the broker | `PendingTransferWorker.sweep` | `lastSweptAt` gating + `.limit(100)` | (compounds the above) |
| `WalletEventsWorker` leak | same file | Removed the `EventEmitter` entirely | Worker memory footprint growing |
| Cache never invalidated | `WalletsService` write paths | Invalidate-on-write, `RedisService` fail-open (§8) | Balance mismatch right after a transaction |
| `deposit()` no transaction | `WalletsService.deposit` | Wrapped in `session.withTransaction`, same idempotency pattern as withdraw | (audit trail gap) |
| Dashboard N+1 | `WalletsService.getDashboard` | One aggregation + one batched `$in` query instead of a loop | Read endpoint doing excess DB work |
| No `ledger_entries` indexes | schema file | `{walletId,createdAt}` + `{transactionId}` | History queries slowing down |
| Correlation ID didn't survive the HTTP layer | multiple files | `AsyncLocalStorage` (§3.5) | Hard-to-investigate incidents |
| `transactions` type-only filter unindexed | schema file | `{type,createdAt}` index | (found during doc write-up, not README-named) |

**"Which one was the highest priority and why?"** The withdraw race,
first — because it's the most direct mechanism behind the literal "gone
negative" symptom, triggers on the most common write path, needs no rare
failure or redelivery to occur (any two concurrent requests), and was
cheap to fix in isolation. The transfer pipeline was comparably severe but
a materially larger piece of work, so it was scoped as its own pass
immediately after.

**"Why did fixing transfer idempotency touch more than just adding a
unique index?"** Because `session.withTransaction` retries its whole
callback on write conflict — a debit that mutates a plain JS object
in-place, or a publish that fires as a direct side effect inside that
callback, both break under a **genuinely concurrent** retry even after you
add an idempotency key at the request level. Closing the four
README-named symptoms correctly required treating the debit, the
idempotency check, and the publish as one unit, not four independent
patches.

---

## 5. The six bonus features

**"Which bonus items did you build, and why those six specifically?"**
Picked for either closing a concrete, already-verified gap that directly
extended work already done, or adding real, cheap value on their own —
explicitly *not* just working down the README's list in order.

1. **Outbox consistency** — `PendingTransferWorker.sweep()` was the one
   remaining direct-RabbitMQ-publish call site; routed through the outbox
   like everything else (§3.3).
2. **Dead-letter queue** — a message that failed twice used to just be
   destroyed (`nack(message, false, false)` with nothing to inspect).
   `RabbitMQService` now asserts a fanout DLX + DLQ and declares the
   transfer queue with an `x-dead-letter-exchange` argument — RabbitMQ
   handles the routing automatically, no consumer-side change needed.
   Deliberately doesn't add automatic reprocessing — inspectability was
   the actual gap.
3. **Wallet reconciliation endpoint** (`GET /wallets/:id/reconcile`) —
   `LedgerService.aggregateNetByWallet()` already existed and was unused;
   compares it against the wallet's stored balance and reports the signed
   drift. Reads Mongo directly, not the cache — reconciliation has to
   check against ground truth.
4. **Ledger audit endpoint** (`GET /wallets/:id/audit`) — one level more
   granular than the transaction-history endpoint: individual debit/credit
   ledger legs, paginated, with an optional `direction` filter.
5. **Rate limiting** — `@nestjs/throttler`, Redis-backed storage
   (`@nest-lab/throttler-storage-redis`, reusing the existing
   `RedisService` connection rather than the library's default in-memory
   store, which wouldn't survive a restart or work across replicas).
   Generous global default (100 req/60s); a tighter override specifically
   on `POST /wallets/transfer` (15 req/10s) — deliberately *not* on
   `/auth/login`, since the integration suite logs in fresh in nearly
   every test.
6. **Inbox pattern** — see §3.4.

**"Any interesting bugs you hit building the bonus features, not related
to the core assessment?"** Two good stories:
- Writing the rate-limiting integration test with 20 truly *concurrent*
  requests via `Promise.all` made the test run unreliable (intermittent
  hangs) under this environment's connection handling. Switched to firing
  them **sequentially** instead — and realized that's actually the more
  correct test design anyway: a rate limiter's threshold behavior is a
  *counting* concern, not a concurrency race the test needs to reproduce;
  the storage adapter's own Redis Lua script already guarantees the
  counter is atomic under real concurrency.
- The new rate-limiting throttle state lives in the same real, shared
  Redis instance across every integration spec file (unlike Mongo, which
  gets wiped per file) — a route hit from more than one file could
  accumulate against the same throttle key. Added a shared
  `flushThrottleState()` test helper, called in every integration file's
  `beforeEach`, to prevent cross-file/cross-run interference.

---

## 6. The four bonus features you deliberately skipped

Know these cold — "why didn't you build X" is an easy trap if your only
answer is "ran out of time."

- **Optimistic locking on the `version` field** — there's no
  read-modify-write call site left to protect. Every write path already
  uses atomic `$gte`-guarded `$inc` updates (§3.1), which closes the exact
  race optimistic locking would close. Wiring it up would protect nothing
  that isn't already safe — it'd be manufacturing a use case, not fixing a
  gap.
- **Distributed tracing** — correlation IDs (§3.5) already give the
  practical debugging value (grep one id across every log line, request
  to consumer) at a fraction of the infrastructure cost. Full tracing
  needs an OTel collector and is a larger investment better justified by
  an actual multi-service topology than one service has today.
- **Prometheus metrics** — doing it well (latency histograms, queue-depth
  gauges, cache hit rates) is a wide surface; doing it shallowly (one
  counter bolted on) doesn't demonstrate much. Better deferred with a
  named list of what the first three metrics would be than built
  superficially.
- **Circuit breaker** — Redis calls already fail open (catch, log, degrade
  to a cache miss — no need to "trip" on an optional cache). RabbitMQ
  reconnection is handled at the connection-manager level already. There's
  no synchronous third-party dependency in this system where a breaker is
  the natural fit — the existing fail-open/reconnect strategies already
  cover the practical failure modes.

---

## 7. Testing strategy

**"How did you verify your fixes actually work, not just that the code
compiles?"**

- **Reproduced bugs before fixing them.** The withdraw race, the transfer
  idempotency gaps, and the dashboard's `.aggregate()` casting bug were
  all confirmed against real, unmodified code first (a pre-existing
  failing test, or a fresh integration test run against the old code)
  before touching anything — so the "fix" is provably closing a real gap,
  not just plausible-looking code.
- **Unit tests with mocked models** for logic/branching (idempotency
  pre-checks, duplicate-key-race fallbacks, the sweep's batching query
  shape, the inbox claim's fast-path/duplicate-key/no-messageId
  branches).
- **Integration tests against a real MongoDB replica set, real Redis, and
  a real RabbitMQ broker** (not Docker in this environment specifically —
  see the "why not Docker" story below) for anything concurrency- or
  delivery-semantics-related, since a mocked model can't demonstrate an
  actual database-level race or an actual redelivered AMQP message.
  Examples: 10 concurrent withdrawal requests via real HTTP calls,
  asserting exactly 5/10 succeed and the balance is exactly 0, not
  negative; forcing a transfer event to fail twice and asserting a message
  actually lands in the DLQ; asserting an index actually gets built by
  querying `getIndexes()`/`.indexes()` against the real collection
  (Mongoose builds declared indexes asynchronously in the background — a
  mocked test can't observe this at all).
- **"Prove the guarantee, not just the happy path."** E.g. for cache
  invalidation: populate the cache via a real `GET`, perform a write,
  assert the very next `GET` reflects the new value immediately — proving
  the actual named symptom is gone, not just that a function was called.

**"Why native MongoDB/Redis/RabbitMQ instead of the repo's
`docker-compose.yml`?"** A pre-existing native `mongod` on this machine
was already bound to `localhost:27017`, so connections meant for the
Docker Mongo container were silently resolving to the native instance
instead — producing "Transaction numbers are only allowed on a replica set
member or mongos" even though a transaction run directly against the
container worked fine. Rather than fight the networking, enabled
`replication.replSetName: rs0` on the native `mongod` (config backed up
first) and used the native Redis/RabbitMQ too. `docker-compose up` still
works as documented for anyone else; this was purely an environment quirk.

---

## 8. Trade-offs and "why not X instead"

- **Why invalidate the cache instead of updating it with the new value?**
  A racing "set" can lose to a stale value (if a concurrent read
  repopulates the cache with an old value *after* your write's own "set"
  runs, that stale value sticks until TTL). A delete is idempotent and
  order-independent — there's no "wrong order" that leaves a bad value
  behind.
- **Why does `RedisService` catch its own errors instead of letting them
  propagate?** Once the write paths started calling Redis at all (to
  invalidate the cache), a Redis outage could otherwise turn an
  already-committed, successful financial write into a client-facing 500.
  Catching and degrading (return `null`/no-op, log it) means a cache
  outage costs you staleness, not availability.
- **Why unique+sparse indexes instead of a data migration?** Deliberately
  accepted as a real-rollout risk, not silently ignored — sparse means
  existing documents without a value are unaffected, but a production
  deployment with two pre-existing documents that *do* share a value would
  fail the index build. Called out explicitly rather than assumed away.
- **Why one bounded retry (RabbitMQ's `redelivered` flag) instead of a
  retry counter stored on the message?** Simpler, and sufficient for closing
  "silent message loss" — the trade-off is a known edge case (a consumer
  crash-then-restart between delivery and ack means the next attempt
  already looks "redelivered" and gets zero extra retries before hitting
  the DLQ), named rather than hidden.
- **Why not build the terminal failed+refund state machine for stuck
  transfers?** It needs attempt-count tracking and a compensating reversal
  transaction type — meaningfully more design work than anything else in
  this pass, and — importantly — is **actively worse than not building it**
  if built without the reversal half (a transfer marked "FAILED" with no
  refund looks resolved while the sender's money is still actually stuck
  debited). Better to ship the sweep-worker/DLQ self-healing path now and
  name the terminal state machine as the next real piece of work, than
  ship a half-finished version that looks done.

---

## 9. What's still broken / remaining debt

Know this list — "what would you fix next" is one of the most common
follow-ups, and having a precise, prioritized answer (not a vague "more
tests") signals real ownership.

In priority order (see `DESIGN.md` §7 for the full write-up):

1. **Terminal failed+refund path for transfers** — the DLQ makes a stuck
   transfer visible; nothing automatically resolves it yet.
2. **A dedup/migration script** for the new unique indexes, for a real
   rollout against existing data.
3. **Multi-instance safety for the background workers** — none of the
   three interval workers has a distributed lock; running more than one
   API replica means every instance's timer fires independently. The
   recommended fix: pull them into their own deployable process (a second
   `NestFactory.createApplicationContext()` entry point, no HTTP listener)
   rather than adding lock/leader-election machinery — removes the
   coordination problem structurally instead of managing it.
4. **The narrow cache read/populate-vs-invalidate race** — a much
   narrower window than the original bug, not fully eliminated; needs a
   version-stamped or compare-and-swap cache write to close completely.
5. **RabbitMQ integration-test isolation** — spec files share one real,
   named queue with no per-file namespacing; a rare, observed
   teardown-timing flake, documented not fixed.
6. **Per-user rate limiting, not per-IP** — `ThrottlerGuard` keys on
   `req.ip` by default even though every request is already
   JWT-authenticated.
7. Observability (tracing/metrics) — see §6.
8. **DLQ reprocessing tooling** — inspectable now, nothing replays a
   dead-lettered message yet.
9. The dead `version` field — should be wired up or removed, not left as
   misleading dead code.

---

## 10. Likely curveball questions

**"What happens if two `sweep()` ticks somehow overlap on the same
transfer?"** The per-transfer transaction includes a `status: PENDING`
guard on the `lastSweptAt` update itself — a transfer no longer `PENDING`
at that instant just doesn't match, so the second sweep's attempt on it
is a safe no-op. Same underlying idea as the consumer's status guard,
applied on the producing side.

**"What if the same RabbitMQ message is delivered to two different
consumer instances at once?"** Can't happen under normal AMQP semantics
for a single queue with competing consumers — a given message is only
ever delivered to (and held by) one consumer until it's acked or nacked.
The scenario this system actually guards against is *redelivery* (the
same message, later, after a nack or requeue), not concurrent delivery.

**"Why does `session.withTransaction` retry matter so much for this
codebase specifically?"** Because it means *any* side effect inside the
callback that isn't itself a plain, idempotent database write (a JS
in-place mutation, a direct network call) can execute more than once for
what looks like a single logical operation. This single fact is the root
cause behind three separate fixes: the transient-retry double-decrement
risk, the direct-publish-inside-transaction bug, and the reason cache
invalidation is placed *after* the transaction block instead of inside
it.

**"If you had to explain the whole session's worth of fixes as one
sentence, what would it be?"** Replace every read-modify-write with an
atomic database-level operation, make every side effect that isn't itself
a database write happen *outside* the transaction that might retry it,
and make every consumer of an at-least-once delivery mechanism safe to
run twice.

**"What was the single most subtle bug you found?"** Probably the
dashboard's `.aggregate()` `$match` comparing a plain string against a
schema-typed `ObjectId` field — `.find()` auto-casts query values to match
schema types, `.aggregate()` does not, so an uncast match silently returns
zero documents (not an error) and every total would have quietly reported
0 regardless of real data. Caught by reviewing a draft before landing it,
confirmed by checking that this exact codebase's own
`LedgerService.aggregateNetByWallet()` already had to cast explicitly for
the identical reason — not a hypothetical risk, a pattern already proven
necessary elsewhere in the same repo.

**"What would you do differently if you started over?"** Probably nothing
structural — the priority order (race conditions → transfer pipeline →
cheap named issues → performance → observability → bonus patterns)
matched both severity and the README's own emphasis. If anything, I'd
consider whether the inbox pattern (§3.4) was worth its cost given it
closes an already-closed gap — a reasonable interviewer pushback, and
worth being able to defend rather than just assert.

---

## 11. Glossary — rapid-fire definitions

- **Outbox pattern**: stage an event as a database row in the same
  transaction as the business write it represents; a separate worker
  relays it to the message broker afterward. Guarantees the write and the
  "intent to publish" can never disagree.
- **Inbox pattern**: record that a specific inbound message has been
  processed (keyed by a message-level id), so redelivery/duplication of
  that exact message is a safe no-op — independent of any business-entity
  status field.
- **Idempotency key**: a caller-supplied (or system-generated) string
  that identifies a *logical* operation, so retrying the same request
  doesn't repeat its side effects.
- **Sparse index**: a MongoDB index that only includes documents where
  the indexed field actually exists — lets you enforce uniqueness on an
  *optional* field without rejecting every document that omits it.
- **`AsyncLocalStorage`**: Node's built-in mechanism for ambient,
  per-async-call-chain storage — lets code deep in a call stack read
  request-scoped data without it being explicitly passed down as a
  parameter.
- **Dead-letter queue (DLQ)**: a separate queue that a message
  automatically routes to (via broker configuration, `x-dead-letter-
  exchange`) when it's rejected/expires from its original queue, instead
  of being silently discarded.
- **`redelivered` flag**: a native AMQP message property set by the
  broker when a message is being delivered again (after a prior nack or a
  consumer disconnect) — distinguishes "first attempt" from "retry" without
  needing your own counter.
- **Fail-open**: on an infrastructure error (e.g. Redis down), degrade to
  a safe default (cache miss) and continue, rather than propagating the
  error and failing the whole request.
- **Write conflict / `TransientTransactionError`**: MongoDB's signal that
  two transactions touched overlapping data and one has to retry — handled
  automatically by `session.withTransaction`'s built-in retry loop.
- **ESR rule** (index design): for a compound index, put Equality fields
  first, then Sort fields, then Range fields — e.g. `{ walletId: 1,
  createdAt: -1 }` serves an equality filter on `walletId` combined with a
  sort on `createdAt`.
