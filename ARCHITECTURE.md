# Architecture

This document describes how the Wallet Platform is put together today: the
services it depends on, how requests flow through the system, and how the
modules communicate with each other. It is a description of the current
system, not a design proposal.

## System context

```
                                   ┌───────────────────┐
                                   │      Clients       │
                                   │ (merchant backends, │
                                   │  internal tooling)  │
                                   └─────────┬──────────┘
                                             │ HTTPS (JWT bearer)
                                             ▼
                                   ┌───────────────────┐
                                   │   Wallet Platform   │
                                   │     (NestJS API)    │
                                   └──┬──────┬──────┬───┘
                         reads/writes │      │      │ publishes
                             ┌────────┘      │      └────────┐
                             ▼               ▼               ▼
                      ┌────────────┐  ┌────────────┐  ┌──────────────┐
                      │  MongoDB    │  │   Redis     │  │   RabbitMQ    │
                      │ (source of  │  │ (balance    │  │ (domain       │
                      │  truth)     │  │  cache)     │  │  events)      │
                      └────────────┘  └────────────┘  └──────┬───────┘
                                                              │ consumes
                                                              ▼
                                                     ┌───────────────────┐
                                                     │ Background workers │
                                                     │ (in-process, same   │
                                                     │  Nest application)  │
                                                     └───────────────────┘
```

The API, the RabbitMQ consumers, and the background workers all run inside
the same NestJS process (see `main.ts`). There is no separate worker
deployment today - `WorkersModule` and `QueueModule` are wired into the same
`AppModule` as the HTTP controllers.

## Request pipeline

Every inbound HTTP request passes through, in order:

1. `CorrelationIdMiddleware` - stamps `X-Correlation-Id` on the request/response.
2. `ApiHeadersGuard`-equivalent auth: the global `JwtAuthGuard` (backed by
   `passport-jwt`), which every route requires unless annotated with
   `@Public()` (used by `POST /auth/login` and `GET /health`).
3. The global `ValidationPipe` (`transform: true`, `whitelist: true`),
   which validates and coerces the DTO for the matched route.
4. The controller/service for the module that owns the route.
5. `LoggingInterceptor` logs method, path, and duration on the way out.
6. `AllExceptionsFilter` catches anything thrown along the way and renders a
   consistent JSON error body.

## Modules

```
src/
├── app.module.ts        # wires every feature module + global providers
├── auth/                 # JWT login, guard, strategy, User schema
├── wallets/              # wallet CRUD, deposit, withdraw, transfer, dashboard
├── transactions/         # append-only transaction records + listing API
├── ledger/                # double-entry ledger entries (debit/credit)
├── outbox/                # transactional outbox for domain events
├── queue/                 # RabbitMQ publisher + transfer event consumer
├── workers/               # in-process background workers (interval based)
├── redis/                 # wallet balance cache client
├── health/                # liveness/readiness endpoint
├── config/                # typed configuration loader
└── common/                # cross-cutting filters, interceptors, middleware
```

### Wallets

`WalletsService` is the primary write path for money movement. It depends on
`TransactionsService` (records an immutable transaction per operation),
`LedgerService` (records the corresponding debit/credit ledger entries),
`OutboxService` (durable, transactional event staging), `RabbitMQService`
(direct publishing), and `RedisService` (balance cache).

- `POST /wallets` creates a wallet and stages a `wallet.created` outbox event
  in the same MongoDB transaction as the wallet document.
- `POST /wallets/:id/deposit` / `POST /wallets/:id/withdraw` update the
  wallet balance and record a matching transaction + ledger entry.
- `POST /wallets/transfer` moves money between two wallets. In one MongoDB
  transaction, it debits the sending wallet (an idempotency key on the
  request is checked first - a retry with the same key returns the original
  transfer instead of debiting again), records the transaction/ledger entry,
  and stages a `transfer.initiated` outbox event, so the receiving side can
  be credited asynchronously once `OutboxRelayWorker` publishes it.
- `GET /wallets/:id` reads through Redis, falling back to MongoDB.
- `GET /wallets/:id/dashboard` returns a wallet summary alongside its
  transaction and ledger history.

### Transactions & Ledger

Every money movement produces one `Transaction` document (the operation
record) and one or more `LedgerEntry` documents (the double-entry
bookkeeping record: a `DEBIT` or `CREDIT` against a specific wallet, with the
resulting `balanceAfter`). `GET /transactions` supports filtering by wallet
and type with pagination.

### Outbox

`OutboxService` persists an `OutboxEvent` document (`routingKey` + `payload`)
in the same MongoDB transaction as the domain write that produced it.
`OutboxRelayWorker` polls for `PENDING` events on an interval, publishes them
to RabbitMQ via `RabbitMQService`, and marks them `PUBLISHED`.

### Queue

`RabbitMQService` owns a single `amqp-connection-manager` connection and
channel, declares the `wallet.events` topic exchange and the
`transfer.events.queue` queue (bound to `transfer.*`), and exposes a
`publish(routingKey, payload)` method used by `OutboxRelayWorker` to deliver
staged events, including `wallet.created` and `transfer.initiated`.

`TransferEventsConsumer` subscribes to `transfer.events.queue` on startup and,
for each `transfer.initiated` message, atomically claims the transfer (an
update conditioned on its status still being `PENDING`, inside a MongoDB
transaction alongside the destination wallet credit and the transaction/ledger
entry) before marking it `COMPLETED` - so a redelivered or duplicate message
is a safe no-op rather than a second credit. A processing failure `nack`s the
message for one redelivery before it's dropped (no dead-letter queue yet).

### Workers

Three interval-based workers run inside the API process:

- `OutboxRelayWorker` - drains pending outbox events to RabbitMQ.
- `PendingTransferWorker` - periodically scans for transfers that have been
  `PENDING` past a configurable timeout and re-publishes `transfer.initiated`
  for each, self-healing transfers whose original event was lost or never
  processed (safe to repeat, since the consumer's status guard makes
  re-processing an already-completed transfer a no-op).
- `WalletEventsWorker` - periodically logs a balance snapshot for the most
  recently updated wallets, for downstream monitoring dashboards.

### Redis

`RedisService` wraps a single `ioredis` client and exposes
`getCachedBalance` / `setCachedBalance` / `invalidateBalance`, keyed per
wallet (`wallet:balance:<id>`) with a configurable TTL.

### Auth

`AuthModule` issues JWTs from `POST /auth/login` against the `User`
collection (bcrypt-hashed passwords). `JwtAuthGuard` is registered globally
via `APP_GUARD` and enforced on every route except those annotated
`@Public()`.

## Data model

```
Wallet 1───* Transaction *───1 LedgerEntry
   │                              │
   └──────────*  Transfer  *──────┘
                    │
                    ▼
              OutboxEvent (staged domain events)
```

- **Wallet**: `userId`, `ownerName`, `currency`, `balance`, `version`.
- **Transaction**: `walletId`, `type` (`DEPOSIT`/`WITHDRAWAL`/`TRANSFER_IN`/
  `TRANSFER_OUT`), `amount`, `status`, `balanceAfter`, optional `reference`,
  `transferId`, `counterpartyWalletId`.
- **LedgerEntry**: `transactionId`, `walletId`, `direction` (`DEBIT`/
  `CREDIT`), `amount`, `balanceAfter`.
- **Transfer**: `fromWalletId`, `toWalletId`, `amount`, `status` (`PENDING`/
  `COMPLETED`/`FAILED`), optional `idempotencyKey`, `failureReason`.
- **OutboxEvent**: `routingKey`, `payload`, `status` (`PENDING`/`PUBLISHED`).
- **User**: `email`, `passwordHash`, `fullName`.

## Deployment

`docker-compose.yml` runs MongoDB (as a single-node replica set, required for
multi-document transactions), Redis, RabbitMQ, and the API image built from
the repository `Dockerfile` (multi-stage, `node:20-alpine`, non-root user).
