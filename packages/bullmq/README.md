# @event-bus-manager/bullmq

Redis-backed transport for Event Bus Manager using `bullmq`.

## Installation
```bash
npm install @event-bus-manager/core @event-bus-manager/bullmq bullmq
```

## Configuration
Requires `redis` configuration (host and port) in the config object.

## Usage
```ts
import { createEventBus } from '@event-bus-manager/core';
import '@event-bus-manager/bullmq'; // Auto-registers 'bullmq' type

const bus = createEventBus({
  type: 'bullmq',
  redis: { host: 'localhost', port: 6379 }
});
```

## Retry & concurrency
Bus-level defaults (`EVENT_BUS_MAX_RETRIES`/`EVENT_BUS_RETRY_DELAY`/`EVENT_BUS_CONCURRENCY`, or per-handler `EventHandler.retry`/`EventHandler.concurrency`) are mapped to BullMQ's own job/worker options when each handler's queue is created:
- `maxRetries` → `attempts: maxRetries + 1` (BullMQ counts the first try; core's `maxRetries` means retries *after* it).
- `retryDelay` (milliseconds) → `backoff: { type: 'fixed', delay: retryDelay }`.
- `concurrency` → `Worker`'s own `concurrency` option (jobs processed in parallel per worker).

## Inbox (idempotent handlers)
Pass an `inboxStore` (e.g. `PgInboxStore` from `@event-bus-manager/pg-store`) in the config to guard every handler against duplicate processing on redelivery — no changes needed in `handle()`. See the root README's "Reliability: Outbox & Inbox" section.
