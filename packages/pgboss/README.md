# @event-bus-manager/pgboss

Postgres-backed transport for Event Bus Manager using `pg-boss`.

## Installation
```bash
npm install @event-bus-manager/core @event-bus-manager/pgboss pg-boss
```

## Configuration
Requires `connectionString` in the config object or via custom overrides.

## Usage
```ts
import { createEventBus } from '@event-bus-manager/core';
import '@event-bus-manager/pgboss'; // Auto-registers 'pgboss' type

const bus = createEventBus({
  type: 'pgboss',
  connectionString: 'postgresql://user:pass@localhost:5432/db'
});
```

## Retry & concurrency
Bus-level defaults (`EVENT_BUS_MAX_RETRIES`/`EVENT_BUS_RETRY_DELAY`/`EVENT_BUS_CONCURRENCY`, or per-handler `EventHandler.retry`/`EventHandler.concurrency`) are mapped to pg-boss's own queue options when each handler's queue is created:
- `maxRetries` → `retryLimit` (same semantics — retries *after* the first attempt).
- `retryDelay` (documented in **milliseconds**, core-wide) → pg-boss's `retryDelay` in **seconds** (converted automatically).
- `concurrency` → `localConcurrency` (workers spawned per node for that queue).

## Inbox (idempotent handlers)
Pass an `inboxStore` (e.g. `PgInboxStore` from `@event-bus-manager/pg-store`) in the config to guard every handler against duplicate processing on redelivery — no changes needed in `handle()`. See the root README's "Reliability: Outbox & Inbox" section.
