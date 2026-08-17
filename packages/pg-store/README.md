# @event-bus-manager/pg-store

Postgres-backed implementations of `OutboxStore` and `InboxStore` from `@event-bus-manager/core` — ready to use, no need to write the claim/lease SQL yourself.

## Installation
```bash
npm install @event-bus-manager/core @event-bus-manager/pg-store pg
```

## Schema
Tables live in their own dedicated Postgres schema (default `event_bus`) rather than `public` — same convention `pg-boss` itself uses (default schema `pgboss`) — so they don't collide with your own application tables named `outbox`/`inbox`.
```ts
import { Pool } from 'pg';
import { ensureSchema } from '@event-bus-manager/pg-store';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await ensureSchema(pool); // creates the "event_bus" schema + `outbox`/`inbox` tables if missing
// custom schema name:
await ensureSchema(pool, 'my_custom_schema');
```

Pass the same `schema` option to `PgOutboxStore`/`PgInboxStore` if you used a custom name:
```ts
const outboxStore = new PgOutboxStore(pool, { schema: 'my_custom_schema' });
const inboxStore = new PgInboxStore(pool, { schema: 'my_custom_schema' });
```

## Outbox — publisher side
Write the event into the outbox table **inside the same transaction** as your business write, instead of calling `bus.publish()` directly.
```ts
import { PgOutboxStore } from '@event-bus-manager/pg-store';
import { createEvent } from '@event-bus-manager/core';
import { UserCreatedV1 } from './events';

const outboxStore = new PgOutboxStore(pool);

async function createUser(input: { email: string }) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      'INSERT INTO users (email) VALUES ($1) RETURNING id',
      [input.email],
    );
    await outboxStore.enqueue(
      client, // the transaction client, NOT the pool
      createEvent(UserCreatedV1, { userId: rows[0].id, email: input.email }),
    );
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
```

Then, in a separate relay process (holding the *real* transport bus):
```ts
import '@event-bus-manager/bullmq';
import { createEventBus, OutboxRelay } from '@event-bus-manager/core';

const realBus = createEventBus({ type: 'bullmq', role: 'publisher', redis: { host: 'localhost', port: 6379 } });
await realBus.start();

const relay = new OutboxRelay(outboxStore, realBus, {
  pollingIntervalMs: 500,
  batchSize: 50,
  leaseMs: 30_000,
});
await relay.start();
```

## Inbox — consumer side
Pass an `inboxStore` in the bus config; handler code doesn't change at all.
```ts
import '@event-bus-manager/bullmq';
import { createEventBus } from '@event-bus-manager/core';
import { PgInboxStore } from '@event-bus-manager/pg-store';
import { SendWelcomeEmail } from './handlers/send-welcome-email';

const inboxStore = new PgInboxStore(pool);

const bus = createEventBus({
  type: 'bullmq',
  role: 'consumer',
  redis: { host: 'localhost', port: 6379 },
  inboxStore,
});

bus.subscribe(new SendWelcomeEmail());
await bus.start();
```

If a handler is redelivered after already completing successfully, it will be skipped automatically — no change needed in `handle()`.

## How claiming works

Both `outbox` and `inbox` rows move through the same shape of state machine:

```
outbox:  PENDING  → PROCESSING (leased) → SENT
inbox:  (no row)  → PROCESSING (leased) → PROCESSED
```

On failure, both go to `FAILED` — but `FAILED` rows stay **reclaimable** (the claim query explicitly includes `status = 'FAILED'` alongside stale `PROCESSING` rows). This is intentional: outbox failures are usually a transient transport outage, and inbox failures must remain reclaimable so the underlying transport's own retry (already wired via `EventHandler.retry`) can redeliver the job and re-attempt `handle()` — a `FAILED` row that couldn't be reclaimed would permanently block that retry. Only `SENT`/`PROCESSED` are true dead-ends.

Claiming uses `FOR UPDATE SKIP LOCKED` (outbox) / `INSERT ... ON CONFLICT DO UPDATE ... WHERE` (inbox), so running multiple relay instances or consumer processes concurrently is safe — they never claim the same row twice.
