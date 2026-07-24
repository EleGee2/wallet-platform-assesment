# Design Notes

## 1. What issues did you find?

- **Withdraw race condition** *(fixed)*: `withdraw()` did a read-modify-write
  (`findById` → mutate in JS → `.save()`), no session, no atomic operator.
  Reproduced for real: 10 concurrent withdrawals of 20 against a balance of
  100 all returned 201, and the persisted balance landed at 0 instead of the
  -100 that "10 successes" implies - a lost-update race.
- **Transfer has no idempotency and no exactly-once completion** *(fixed)*:
  no unique constraint on `Transfer.idempotencyKey`; the RabbitMQ consumer
  didn't check `transfer.status` before crediting (duplicate credit on
  redelivery); all consumer errors were `ack`'d (silent message loss,
  transfers stuck `PENDING` forever); the sweep worker only logged stale
  transfers, never resolved them; the debit and the publish both sat inside
  a Mongo transaction that retries on write conflict, so a genuinely
  concurrent retry could double-debit and double-publish. A currently-failing
  unit test and a currently-failing integration test were direct, unmodified
  evidence of this.
- **Sweep worker could flood the broker** *(fixed)*: the sweep query matched
  on `createdAt` only, with nothing recorded after a re-publish - a still-stuck
  (or poison) transfer would be re-published on every sweep tick (5s default)
  instead of once per timeout window (60s default), and the query itself had
  no `.limit()`. Found via an external code review of the sweep worker after
  the transfer fix above landed; both points checked out against the actual
  code and fixed. A third point from that same review - a concurrent-execution
  race requiring a `PENDING -> PROCESSING` intermediate state - does **not**
  apply: the consumer's atomic status-guarded claim (above) already closes
  that window; verified by re-reading `completeTransfer()`, not taken on
  faith.
- **`WalletEventsWorker` unbounded listener leak** *(fixed)*: re-registered
  an `EventEmitter` listener per active wallet every 10s tick, never
  removed; `setMaxListeners(0)` masked Node's own leak warning. Separately,
  the tick itself had no `.catch()` around its async body inside
  `setInterval` - a transient failure was an unhandled promise rejection,
  not just a missed log line.
- **Wallet balance cache never invalidated** *(fixed)*: `deposit`,
  `withdraw`, and the transfer consumer's credit step never touched Redis;
  `invalidateBalance` was dead code, called from nowhere. Directly explained
  "balance doesn't match the app" reports - `GET /wallets/:id` trusted a
  cached value over a fresh Mongo read for up to the full TTL after any
  write.
- **`deposit()` not wrapped in a transaction** *(fixed)*: wrote balance +
  transaction + ledger as three independent, non-sessioned writes - a crash
  mid-sequence broke the audit trail. Also exposed a live regression: once
  `Transaction.reference`'s unique index was added (for withdraw's
  idempotency fix), a retried deposit reusing a `reference` hit that index
  at `transactionsService.create()` uncaught - a 500 instead of a clean
  result, since deposit had no duplicate-key handling of its own.
- **Dashboard endpoint's unbounded N+1 query pattern** *(fixed)*: fetched a
  wallet's entire transaction history, then issued one ledger query per
  transaction in a loop - `2 + N` round trips where N is lifetime
  transaction count - discarding almost all of it to show the last 10. Had
  zero test coverage before this fix (no unit or integration tests).
- **No indexes on `ledger_entries`** *(fixed)*: every read path
  (`getEntriesForWallet`, `computeBalanceFromLedger`, `aggregateNetByWallet`,
  and the dashboard's batched lookup) filtered on `walletId` or
  `transactionId` against a collection with zero declared indexes - full
  collection scans that get more expensive as ledger volume grows (2x
  transaction volume, by construction).
- **`type`-only transaction filters had no supporting index** *(fixed)*:
  `QueryTransactionsDto` allows filtering by `type` with no `walletId`
  (across every wallet), but the only index on `transactions` was
  `{ walletId: 1, createdAt: -1 }` - a `type`-only query couldn't use it at
  all and fell back to a full collection scan. A separate, lower-priority
  gap from the `ledger_entries` one above (different collection), picked up
  once the rest of the design doc's write-up prompted a second look.
- `version` field on `Wallet` is dead code - never read, checked, or
  incremented, despite a comment claiming it's for optimistic concurrency.
- **Correlation ID didn't survive past the HTTP layer** *(fixed)*: generated
  per request but only stashed on the Express `req` object - never read
  into the interceptor's or exception filter's own log lines, and
  structurally unable to reach the outbox payload, the AMQP message, or the
  consumer's logs, since none of those carried a field for it. You could not
  take one request's correlation ID and grep logs for everything it caused
  downstream.

## 2. What did you prioritize, and why?

**`withdraw()`'s balance race, first** - the direct mechanism behind "wallet
balances have occasionally gone negative under load," on the most common
write path in the system, triggered by any two concurrent requests (no rare
failure or redelivery needed), and a cheap, contained fix (one method, no
cross-service coordination). Folded in idempotency-key enforcement on the
same request since it needed the same session/transaction plumbing.

**The transfer pipeline, second** - comparable severity but a materially
larger piece of work, so scoped as its own pass. It's the single largest gap
in the system: it covers three of the README's "Known Production Issues" at
once (duplicate side effects, transfers stuck forever, the transfer feature
left unfinished) and is the area most likely to be probed by hidden tests
given how explicitly the README calls it out. Fixing it correctly - not just
patching the four symptoms independently - required treating the debit,
the idempotency check, and the publish as one unit: `session.withTransaction`
retries its whole callback on a write conflict, so a debit that mutates a
JS object in place, or a publish that fires as a direct side effect, both
break under a genuinely concurrent retry even after "fixing" idempotency at
the request level. That's why this pass touches more than the four gaps
literally named in the issue - the alternative was a fix that looks correct
against a sequential retry but reopens the same class of bug under real
concurrency.

**`WalletEventsWorker`'s leak, third** - cheap (one file, no cross-service
coordination) and a named production issue, so picked up as soon as the
transfer pipeline work was done rather than left for "another day."

**Cache invalidation, fourth** - the other cheap, named-issue item queued up
in "what would you improve with another day" from the previous pass. Adding
it required making `RedisService` itself resilient to a Redis outage first
(see Trade-offs) - not extra scope, but a direct safety requirement of
calling Redis from the write path at all, something none of these methods
did before.

**`deposit()`'s transaction wrapper, fifth** - the last "three independent
writes" gap, and cheap given `withdraw()`'s exact pattern already existed to
copy. Pulled the idempotency pre-check/duplicate-key handling along with it
(not just the transaction wrapper) - once `Transaction.reference` became a
global unique index, deposit needed that handling to not 500 on a retried
request, the same "partial parity is actually incorrect" reasoning as the
transfer fix.

**Dashboard's N+1 query, sixth** - a pure read-path performance fix (no
correctness bug in the original, just wasteful), so lower urgency than
anything above, but cheap once a draft rewrite existed to review rather
than write from scratch.

**`ledger_entries` indexes, seventh** - the cheapest item on the whole
list ("very low" fix cost, no application code changes) and directly
underneath the query pattern the dashboard fix had just touched, so done
immediately after rather than deferred.

**Correlation ID, eighth** - the last of the originally-catalogued P1s.
Genuinely "medium" cost, as flagged up front: it touches request
middleware, the interceptor, the exception filter, both schemas that stage
async work, the queue publisher, both workers that publish, and the
consumer - more files than any single fix so far this session, though each
individual change is small (an `AsyncLocalStorage` read/write, one new
field, one new parameter). Scoped deliberately to what actually carries a
real request's ID (HTTP logs, the outbox event, the AMQP message, the
consumer, and - since a stuck transfer is exactly when this matters most -
the sweep worker's republish too) rather than inventing a synthetic
per-tick ID for workers whose actions aren't triggered by any single
request.

## 3. How did you handle concurrency?

**Where two requests can race**: two concurrent `withdraw`s on the same
wallet (fixed); two concurrent `transfer`s with the same idempotency key
(fixed); the transfer consumer processing the same message twice, via
redelivery or the sweep worker's republish (fixed); a concurrent cache
populate racing a concurrent invalidation (narrowed, not fully closed - see
Data Consistency below).

**Withdraw**: balance check-and-decrement is one atomic command
(`findOneAndUpdate({ balance: { $gte: amount } }, { $inc: { balance:
-amount } })`) instead of a read, a JS check, and `.save()`. Wallet update,
idempotency check, transaction record, and ledger entry are one Mongo
transaction. A `reference` on the request is checked before the mutation,
and `Transaction.reference` is a unique + sparse index - a concurrent
double-submission that slips past the pre-check still collides at the
database level, converted to a 409 (rolling back the decrement, same
transaction).

**Transfer creation**: same atomic-debit pattern
(`findOneAndUpdate`/`$gte`/`$inc`) replaces the old `fromWallet.balance -=
amount; save()`. `idempotencyKey` is checked before any wallet reads (a pure
replay costs one lookup, no debit) and `Transfer.idempotencyKey` is now
unique + sparse; a concurrent race that slips past the pre-check hits the
index instead, and - unlike withdraw - returns the *original* transfer
rather than an error, matching the field's own documented contract ("retried
requests should reuse the same key"). The `transfer.initiated` publish moved
from a direct `RabbitMQService.publish()` call to `OutboxService.enqueue()`
in the same session: a direct publish is a real side effect that would fire
on both an aborted attempt and its automatic retry, while an outbox write is
just another Mongo write and rolls back with the rest of the transaction.

**Transfer completion (consumer)**: replaced the fetch-then-`.save()`
pattern with an atomic, status-guarded claim -
`findOneAndUpdate({ _id, status: PENDING }, { status: COMPLETED })` - before
crediting the destination wallet (also via `$inc`, not read-modify-write). A
null result (not found, or already handled) is a safe no-op. This is what
makes redelivery, duplicate messages, and the sweep worker's republish all
safe by construction, and it's the same atomic-guard idea as withdraw's fix,
applied on the consuming side of the queue.

**Sweep worker frequency**: the sweep query now also requires
`lastSweptAt` to be missing or older than the timeout window
(`$or: [{ lastSweptAt: { $exists: false } }, { lastSweptAt: { $lt: cutoff } }]`),
and bulk-sets `lastSweptAt` for the whole matched batch *before* attempting
any publish - so a still-stuck transfer is retried once per timeout window
(matching the original intent) instead of once per sweep tick, and a
persistently-unreachable broker backs off the same way rather than being
hammered every tick. The query also gained a `.limit(100)`, matching
`OutboxRelayWorker.findPending(50)`'s existing batching convention, so a
large backlog (e.g. after a multi-hour broker outage) can't load an unbounded
number of documents into memory at once.

**On the "concurrent execution" claim specifically**: a follow-up review
argued the sweep needs a `PENDING -> PROCESSING` intermediate state to avoid
two workers executing the same transfer concurrently. Checked against the
actual code and rejected: `completeTransfer()`'s claim (`findOneAndUpdate({
_id, status: PENDING }, { status: COMPLETED })`) already *is* that guard - two
concurrent attempts for the same transfer hit a genuine MongoDB write
conflict inside their respective transactions, and `session.withTransaction`
retries the loser automatically, which then sees the already-`COMPLETED`
status and no-ops. No additional state needed.

**Cache invalidation**: `deposit`, `withdraw`, `transfer` (debit), and the
transfer consumer (credit) each now call `RedisService.invalidateBalance`
after their write actually commits. For the two transactional paths
(withdraw, transfer), this call is placed *after* the `session.
withTransaction(...)` block completes, never inside its callback - the same
reasoning as moving the outbox publish out of the retried callback: an
aborted-then-retried attempt would otherwise invalidate the cache before the
real commit happens, leaving a window where a concurrent read repopulates
the cache with a value that's about to go stale again, with nothing left to
invalidate it a second time. Invalidation (not "set the new value") was the
deliberate choice specifically to avoid that class of race - a delete is
idempotent and order-independent; a racing "set" is not.

**`deposit()`**: now mirrors `withdraw()` exactly - idempotency pre-check,
atomic `$inc` (no `$gte` guard needed here; deposits have no lower bound),
`Transaction`/`LedgerEntry` writes, and cache invalidation all in one
`session.withTransaction`, with the same duplicate-key-to-409 catch as a
backstop against a concurrent request racing past the pre-check.

**Dashboard endpoint** : replaced
the full-history loop with 4 fixed queries - `.lean()` wallet read; one
aggregation computing `transactionCount`/`totalDeposited`/`totalWithdrawn`
over the wallet's *entire* history in the database instead of a Node loop;
a `.limit(10)` query for the transactions actually shown; one batched `$in`
query for their ledger entries instead of one per transaction. A draft of
this rewrite was reviewed before landing, and one real bug was caught and
fixed first: the draft's aggregation `$match` compared a plain string
against `Transaction.walletId` (schema-typed `ObjectId`) - `.find()` casts
query values to match schema types automatically, `.aggregate()` does not,
so uncast this would have silently matched zero documents and returned 0
for every total regardless of real data. Confirmed via this same
codebase's own `LedgerService.aggregateNetByWallet()`, which already casts
explicitly for the identical reason - not a hypothetical, a pattern already
proven necessary here.

**`ledger_entries` indexes**: `{ walletId: 1, createdAt: 1 }` (serves
`getEntriesForWallet`'s filter+sort, `computeBalanceFromLedger`'s and
`aggregateNetByWallet`'s filter, since a compound index also serves
equality queries on its prefix field) and `{ transactionId: 1 }` (serves
the dashboard's batched `$in` lookup). Verified these actually get built,
not just declared: booted the app against the real MongoDB instance and
inspected `db.ledger_entries.getIndexes()` directly - both indexes were
present (Mongoose builds declared indexes in the background on connect;
confirming this against a real collection is the only way to know the
declaration actually took effect, since a mocked unit test can't observe
an index at all).

**Correlation ID** : a single
shared `AsyncLocalStorage<{ correlationId }>` (`src/common/context/
request-context.ts`) is the one primitive everything else reads from -
no threading an id through method signatures anywhere. `CorrelationId
Middleware` now runs the rest of the request pipeline inside it instead of
just stashing the id on `req` (nothing else read that field, confirmed via
grep, so it's removed rather than kept as a second, divergent mechanism);
`LoggingInterceptor` and `AllExceptionsFilter` read it into their existing
log lines. `OutboxEvent` and `Transfer` both gained a `correlationId`
field, populated ambiently (`OutboxService.enqueue()` and `transfer()`'s
`Transfer.create()` just read `getCorrelationId()` - no call-site changes
needed anywhere in `WalletsService`). `RabbitMQService.publish()` takes an
optional third parameter, passed through to AMQP's own native
`correlationId` message property (confirmed in `amqplib`'s type
definitions - a real 0-9-1 message property, not a custom header).
`OutboxRelayWorker` and `PendingTransferWorker` both pass their record's
stored `correlationId` through on every publish call, and
`TransferEventsConsumer` reads `message.properties.correlationId` and
re-establishes the context around its own processing, so its log lines
(including the existing "Transfer completed" line) carry the same id the
original HTTP request generated - closing the loop from request to async
consumer.

**Guarantee**: no lost updates and no double-processing under any
interleaving of concurrent requests or message deliveries on the same
wallet/transfer - a structural guarantee from MongoDB's single-document
atomicity plus the status/balance guards, not "less likely." Does **not**
cover anything cache-related. A message that fails twice in a row is
dead-lettered rather than dropped or retried indefinitely (§9).

**Verification**: proved against a real MongoDB replica set + a real
RabbitMQ broker, not just mocked-model unit tests.
`test/integration/concurrency.integration-spec.ts` (unmodified) reliably
reproduced the withdraw race before the fix (10/10 withdrawals of 20
succeeded against a balance of 100, final balance came out to 0, not -100)
and passes deterministically after (exactly 5/10 succeed, balance exactly
0). `test/integration/transfer-flow.integration-spec.ts`'s existing
PENDING→COMPLETED test failed before this fix (receiver never credited) and
passes after; added two new integration tests proving no double-debit on a
repeated idempotency-key request and no double-credit when the same
`transfer.initiated` event is redelivered. Added a further integration test
for the sweep worker specifically: a transfer inserted directly (bypassing
the normal create path, so no event was ever published for it) gets
re-published exactly once across two immediate, back-to-back manual `sweep()`
calls, and still completes normally afterward. Added unit tests for both
services' idempotency pre-checks, duplicate-key-race fallbacks, the
consumer's redelivery/failure-handling paths, and the sweep worker's
batching/gating query shape. For cache invalidation: added
`test/integration/wallets.integration-spec.ts` and
`transfer-flow.integration-spec.ts` cases that populate the cache via a real
`GET`, perform a write, and assert the very next `GET` reflects the new
balance immediately - proving the actual named symptom ("balance doesn't
match the app right after a transaction") is gone, not just that a function
was called. Added `redis.service.spec.ts` (none existed) covering both the
happy path and that a Redis failure degrades to a no-op/cache-miss instead
of throwing. For `deposit()`: added the same idempotency-suite unit tests as
withdraw, plus an integration test proving the fix against the real unique
index - the same deposit `reference` posted twice returns 409 on the
second attempt, the wallet is only credited once, and exactly one
`Transaction` document exists for that reference. For the dashboard
endpoint (previously zero coverage - added both unit and integration
tests from scratch): unit tests assert the aggregation's `$match` receives
a real `Types.ObjectId` (the regression guard for the bug above), that
`transactionCount` reflects the full lifetime count even when
`recentActivity` is capped at 10, and the zero-transaction-wallet case. The
integration test creates 15 real transactions against real MongoDB and
asserts the totals/count/recentActivity are all correct - this is exactly
the test that would have caught the `$match` casting bug for real (a
broken match returns 0 for everything; this test fails loudly instead of
silently passing). For correlation ID: added first-ever coverage for
`common/middleware`, `common/interceptors`, `common/filters`,
`outbox.service.ts`, and `outbox-relay.worker.ts` (all previously
zero-coverage, confirmed via directory listing before starting), each
proving its own link in the chain reads/writes the id correctly. The one
test that proves the *whole* chain, not just a link: an integration test
sends a real HTTP transfer request with a custom `X-Correlation-Id` header,
confirms the created `Transfer` document stored it, then - since log
output is the actual point of this fix - spies on `Logger.prototype.log`
and confirms at least one captured line from the async consumer processing
the event contains that exact id after polling to `COMPLETED`.

One flake observed while re-running the integration suite during this pass,
unrelated to any of the above: `rabbitmq.integration-spec.ts`'s smoke test
publishes a deliberately malformed `transferId: 'test-transfer'` payload
that's never cleaned up, and RabbitMQ's queue durability means it can
resurface in a *later* test file's run. Integration spec files each start
their own app/consumer but share the same real, named queue, and Jest can
run separate spec files as parallel workers - so that leftover message can
get redelivered into a different file's consumer right as its `app.close()`
is tearing down the channel, throwing `IllegalOperationError: Channel
closing` from inside `nack()`. Reproduced once, didn't reproduce on an
immediate re-run - a pre-existing test-isolation gap (no per-file queue
namespacing, no cleanup of the smoke test's own bad message), not something
this pass introduced or fixed.


## 4. How did you ensure data consistency?

- **MongoDB writes**: fixed across the board now - `deposit`, `withdraw`,
  and `transfer` (creation and completion) each commit their balance,
  transaction, and ledger entry as one transaction.
- **Cache**: fixed for the common case - every write path now invalidates
  the affected wallet's cache entry after it commits. One narrower residual
  race remains: `getWallet()`'s cache-miss path reads Mongo, then populates
  the cache from that read; if a write's invalidation lands in the gap
  between that read and the populate call, the populate can still write a
  soon-to-be-stale value back in, with nothing left to invalidate it again
  until TTL expiry. This is a much narrower window than the original bug
  (which was deterministic on every write), but not eliminated - closing it
  fully would need a version-stamped or compare-and-swap cache write, which
  felt disproportionate to add here.
- **Message queue**: both places that publish `transfer.initiated` - the
  original creation path and the sweep worker's republish (§9) - now go
  through the outbox instead of firing directly, and the consumer is
  idempotent against redelivery. A failed message gets exactly one retry,
  then is dead-lettered rather than dropped (§9). Still open:
  `OutboxRelayWorker` itself is still at-least-once, not exactly-once
  (acceptable now that the consumer tolerates duplicates, but worth naming).

## 5. Trade-offs

- **Latency**: withdraw and transfer both gain one extra query in the common
  path (the idempotency pre-check, skipped when no key is supplied).
  Routing transfer's publish through the outbox adds up to
  `outboxRelayIntervalMs` (~2s default) of extra latency before the event
  reaches RabbitMQ at all, versus firing immediately - a real cost, traded
  for the publish no longer being able to fire twice on a transaction retry.
- **Complexity**: both fixes add a transaction wrapper and a nested
  try/catch converting a driver-level duplicate-key error into a domain
  result - real readability cost, but load-bearing (it's what closes the
  concurrent-double-submission case, not just sequential retries).
- **Migration risk**: `Transaction.reference` and `Transfer.idempotencyKey`
  are now unique + sparse. Sparse means documents without a value are
  unaffected (the common case today). Risk is any existing deployment with
  two documents already sharing a non-null value - the index build would
  fail. No production data to check against; a real rollout would need a
  dedup pass first.
- **Conservative choices**: didn't wire up the `version` field for
  optimistic locking as an alternative to the atomic `$gte`-guarded `$inc` -
  simpler, no retry loop, same guarantee for a single-field case. The
  retry-then-dead-letter policy is one bounded retry via RabbitMQ's own
  `redelivered` flag, not a retry count stored on the message - a known edge
  case: a consumer crash-then-restart between delivery and ack means the
  next attempt already looks "redelivered" and gets zero extra retries
  before landing in the DLQ. Didn't build a terminal "mark FAILED + refund"
  path for a transfer still stuck after that retry - the sweep worker
  re-publishes it once per timeout window instead, indefinitely (and now a
  message that fails twice is at least preserved in the DLQ, §9, rather than
  only relying on the sweep), which is a real, working resolution (safe now
  that the consumer is idempotent) but not a terminal one; a full terminal
  state machine needs attempt-count tracking and a compensating reversal
  transaction type, which felt disproportionate to this pass, and -
  importantly - is actively worse than doing nothing if built without the
  reversal (a "FAILED" transfer with no refund looks resolved while the
  sender's money is actually still stuck debited).
- **`WalletEventsWorker`**: the fix removes the `EventEmitter` entirely
  (nothing is ever registered, so there's nothing to leak) rather than
  trying to remove listeners correctly - simpler and cheaper than making the
  original pattern safe. Added `.lean()` to its query (it only reads
  `_id`/`balance` for logging, never touches Document methods, so skipping
  hydration is free) and an index on `Wallet.updatedAt` the query already
  depended on but didn't have. Deliberately didn't add an `OutboxRelayWorker`-
  style overlap guard (`running` flag) - that guard protects against
  double-publishing side effects; this tick is a single bounded read with no
  writes, so overlap risk is just a duplicate log line, not worth the extra
  surface area for this fix.
- **Cache invalidation**: `RedisService`'s three methods now catch their own
  errors and degrade (return `null` / no-op) instead of throwing - required
  so a Redis outage can't turn an already-committed financial write into a
  client-facing 500, now that the write paths actually call Redis. As a
  direct, free consequence this also closes the previously-separate "no
  fallback when Redis is down" gap on `getWallet()`'s read path - no changes
  needed there, since a caught error and a genuine cache miss are now
  indistinguishable to that method. Chose invalidate-on-write over
  set-on-write specifically to avoid a second race (a racing "set" can win
  with a stale value; a delete can't).

## 6. Remaining technical debt

- The narrow read/populate-vs-invalidate cache race described above (Data
  Consistency) is still open.
- `version` field is still dead code with a misleading comment.
- A transfer stuck past its one retry now lands in the dead-letter queue
  (§9) instead of the sweep worker's once-per-timeout-window republish being
  the only path back - but there's still no automatic reprocessing or
  terminal failed+refund path; a dead-lettered message needs manual/future
  handling.
- The sweep query is still an unindexed scan on `transfers` (pre-existing,
  not introduced by the frequency fix - not addressed here to keep that
  change scoped).
- The unique-index migration risk above (both `Transaction.reference` and
  `Transfer.idempotencyKey`) is unverified against real data.
- Integration test isolation: spec files share one real, named RabbitMQ
  queue with no per-file namespacing and no cleanup of the smoke test's
  deliberately-malformed message - observed once as a rare teardown-timing
  flake (see Verification above), not yet fixed.
- Purely time-triggered worker actions with no originating request
  (`WalletEventsWorker`'s snapshot tick, `OutboxRelayWorker`'s own
  batch-level failure log) still have no contextual id of their own -
  deliberately out of scope for the correlation ID fix (there's no request
  to trace back to), but still means those specific log lines can't be
  correlated across their own multi-line actions the way request-triggered
  ones now can.

## 7. What would you improve with another day?

In priority order - realistically, items 1-3 are what would actually get
done in a single day; the rest is the honest longer backlog beyond that,
not padding.

1. **Terminal failed+refund path for transfers** - the dead-letter
   queue makes a stuck transfer *visible*, but nothing automatically
   resolves it. Needs attempt-count tracking on `Transfer`, a terminal
   `FAILED` status, and a compensating reversal transaction type that
   credits the sender back. The single largest remaining design gap.
2. **A dedup/migration script for the new unique indexes** -
   `Transaction.reference` and `Transfer.idempotencyKey` are unique+sparse
   now; a real rollout against existing data needs a one-time pass to find
   and resolve any pre-existing duplicates before the index build, not
   just a dev-environment assumption.
3. **Multi-instance safety for the background workers** - `PendingTransferWorker`,
   `WalletEventsWorker`, and `OutboxRelayWorker` each just start their own
   `setInterval` with no distributed lock or leader election. Running more
   than one replica of this service (a normal scaling step) means every
   instance's timer fires independently - redundant work at best, a fresh
   contention source at worst. Not exercised by anything in this repo
   today, but a real gap before this could actually scale horizontally.
   The fix I'd actually reach for isn't a distributed lock (real
   coordination overhead, still fragile under a split-brain) but a
   structural one: pull `WorkersModule` (and arguably `TransferEventsConsumer`
   too, so consumer capacity doesn't have to scale in lockstep with HTTP
   capacity) out of the HTTP `AppModule` entirely, into its own deployable
   process - a second entry point built with `NestFactory.
   createApplicationContext()` (no HTTP listener) instead of `NestFactory.
   create()`, run as its own container/service alongside the API. That
   removes the coordination problem structurally - exactly one worker
   process exists regardless of how many API replicas are running - rather
   than adding machinery to manage N *potential* worker instances safely.
4. **The narrow cache read/populate-vs-invalidate race** - closing it
   fully needs a version-stamped or compare-and-swap cache write.
5. **RabbitMQ integration-test isolation** - per-file queue
   namespacing (or a purge between files) instead of one shared named
   queue, removing the one flaky teardown path that's currently only
   documented, not fixed.
6. **Per-user rate limiting, not just per-IP** - `ThrottlerGuard` keys on
   `req.ip` by default; since every request is already authenticated via
   JWT, keying on the token's subject instead would stop multiple users
   behind the same NAT/proxy from sharing one budget, and stop a single
   bad actor from bypassing the limit by rotating IPs.
7. **Observability**: distributed tracing and Prometheus metrics.
8. **Dead-letter-queue reprocessing tooling** - the DLQ makes failed
   messages inspectable, but nothing replays them; an admin
   endpoint/CLI to requeue a dead-lettered message once a human confirms
   the underlying issue is fixed would close the loop.
9. **`version` field** (§1, §5) - either wire it into a real
   optimistic-locking call site if one ever appears, or remove it
   outright; it's misleading dead code either way.

## 8. Assumptions

- Treated `WithdrawDto.reference` and `TransferDto.idempotencyKey` as the
  intended idempotency keys (per their own doc comments) and global
  uniqueness (not per-wallet) as the right scope, since both are
  caller-supplied strings. Deliberately handled them differently on replay -
  withdraw throws a 409, transfer returns the original result - because only
  `idempotencyKey`'s docstring promises "retried requests should reuse the
  same key."
- Assumed it's acceptable to add unique indexes with no data migration
  script, given this is a dev environment with no production data to
  reconcile first (flagged above as a real-rollout risk).
- Assumed one bounded retry (via RabbitMQ's `redelivered` flag) plus
  indefinite sweep-worker republishing was an acceptable terminal design for
  this pass, rather than a full failed+refund state machine - explicitly
  named as a reasonable next step, not an oversight (the dead-letter queue
  itself was picked up afterward as a bonus item, §9).
- Assumed the caller is responsible for its own retry/backoff on a `409` -
  didn't add client-side or queue-based retry logic.

## 9. Bonus tasks implemented

- **Outbox consistency**: `PendingTransferWorker.sweep()` was the one
  remaining call site publishing to RabbitMQ directly instead of through the
  outbox - the same class of gap the transfer-publish fix (§3) closed,
  just on the republish path. Each stale transfer's `lastSweptAt` update and
  its `transfer.initiated` outbox stage now happen in one Mongo transaction,
  so a crash between them can't silently lose the republish until the next
  full timeout window. The sweep runs on a timer, outside any request, so it
  re-establishes the transfer's own stored `correlationId` via the
  request-context primitive before staging, rather than losing it
  (`OutboxService.enqueue()` reads it ambiently).
- **Dead-letter queue**: a transfer event that failed processing twice used
  to be nacked-without-requeue and simply destroyed, with nothing but a log
  line left behind. `RabbitMQService` now also asserts a fanout dead-letter
  exchange and queue, and declares the transfer queue with an
  `x-dead-letter-exchange` argument, so a message that exhausts its one
  retry lands in an inspectable DLQ instead of vanishing. Doesn't add
  automatic reprocessing - inspectability was the actual gap being closed;
  the refund/reprocessing path is still open (§6).
- **Wallet reconciliation endpoint**: `LedgerService` already had
  `aggregateNetByWallet()` written and unused by any endpoint. Added
  `GET /wallets/:id/reconcile`, comparing the wallet's stored balance
  against that ledger-derived total and reporting the signed drift - the
  most direct way to demonstrate the double-entry ledger design actually
  pays for itself, and cheap since the computation already existed. Reads
  Mongo directly rather than the cache, since reconciliation must check
  against ground truth.
- **Audit endpoint**: `TransactionsController` exposed transaction-level
  history only; nothing surfaced individual ledger legs (`direction`,
  `balanceAfter` per entry) - the actual audit-grade detail the double-entry
  design is meant to provide. Added `GET /wallets/:id/audit`, paginated the
  same way `TransactionsService.findAll` already is, with an optional
  `direction` filter.
- **Rate limiting**: no throttling existed anywhere. Added `@nestjs/
  throttler` with a generous global default (100 req/60s) and a tighter
  override on the money-movement endpoint, `POST /wallets/transfer`
  specifically (15 req/10s) - not `/auth/login`, since the integration
  suite logs in fresh in nearly every test's `beforeEach` and a tight login
  limit would start failing that suite partway through any file with more
  than a handful of tests; a real security posture that doesn't already
  exist isn't being weakened by leaving it out. Backed by Redis
  (`@nest-lab/throttler-storage-redis`, reusing `RedisService`'s existing
  connection) rather than the library's default in-memory store, which
  would silently reset on every restart and wouldn't work at all across
  more than one instance.
- **Inbox pattern**: `TransferEventsConsumer` already had a fully correct,
  atomic status-guard (`findOneAndUpdate({ status: PENDING })`) making
  redelivery of the same transfer safe - this is defense-in-depth on an
  already-closed gap, not a fix for a live one. Added a generic,
  message-level "was this exact delivery already processed" check,
  decoupled from any one business entity's status field: `RabbitMQService.
  publish()` now mints a fresh `messageId` (a real AMQP property) on every
  actual network publish, and the consumer claims it in a new
  `inbox_messages` collection before doing any business work - inside the
  same transaction as the rest of the completion, so a genuinely failed
  attempt (e.g. destination wallet not found) rolls the claim back too,
  instead of "poisoning" that message id against a legitimate retry.

Also considered and deliberately not built: optimistic locking on
`Wallet.version` has no read-modify-write call site left to protect, now
that every write path already uses atomic `$inc`-guarded update) -
wiring it up would protect nothing that isn't already safe. Distributed
tracing and Prometheus metrics are both large enough surfaces that a
shallow version wouldn't demonstrate much more than "a package was
imported." A circuit breaker adds little on top of Redis's existing
fail-open behavior and RabbitMQ's own reconnection handling, since neither
has a synchronous third-party dependency to protect against.

Verified the same way as everything above: unit tests for the sweep
worker's atomic stage-and-mark and correlation-id propagation, a new
`rabbitmq.service.spec.ts` (first coverage) asserting the DLX/DLQ topology
and the `messageId` property, `reconcileWallet`'s and `getAudit`'s
matched/drifted/paginated/not-found cases, and the inbox claim's
fast-pre-check/duplicate-key-race/no-messageId-fallback branches. Integration
tests against the real broker/DB/cache: a transfer event forced to fail
twice lands in the DLQ; the sweep's outbox-staging is gated to once per
timeout window the same way the direct-publish version was proven earlier;
reconciliation reports zero drift after a real mix of deposits/withdrawals/
transfers and the exact drift when the stored balance is mutated
out-of-band; the audit endpoint's paginated entries match what a real
sequence of writes actually produced; a dedicated rate-limiting spec proves
the `/wallets/transfer` override actually returns `429` past its limit
while an unrelated route stays unaffected in the same window; a real
transfer completion claims exactly one `inbox_messages` document. One
practical lesson from writing the rate-limiting test: fired as 20 truly
concurrent requests, the run became unreliable (intermittent hangs) under
this environment's connection handling; sending them sequentially instead
is not just simpler but more representative, since a rate limiter's
threshold behavior is a counting concern, not a concurrency race - the
Redis Lua script the storage adapter uses already guarantees the counter
itself is atomic under real concurrency, so a test doesn't need to
re-prove that.
