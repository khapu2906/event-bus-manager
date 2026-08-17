# @event-bus-manager/mysql-store

MySQL-backed implementations of `OutboxStore` and `InboxStore` from `@event-bus-manager/core` — for services whose business database is MySQL, independent of which transport (`bullmq`, `pgboss`) you actually publish through.

## Installation
```bash
npm install @event-bus-manager/core @event-bus-manager/mysql-store mysql2
```
Requires MySQL 8.0+ (needs `SELECT ... FOR UPDATE SKIP LOCKED`).

## Schema
Tables are created in **whatever database your connection is already using** — `event_bus_outbox`/`event_bus_inbox` by default (prefix, not a separate database). MySQL has no Postgres-style sub-database schema (`CREATE SCHEMA` is a synonym for `CREATE DATABASE` in MySQL), and a genuinely separate database would mean your app's DB user needs a server-level `CREATE DATABASE` privilege plus cross-database transaction support — many managed MySQL platforms (e.g. PlanetScale) restrict or don't support that at all. A table prefix avoids colliding with your own `outbox`/`inbox` tables without asking for any extra privileges.
```ts
import mysql from 'mysql2/promise';
import { ensureSchema } from '@event-bus-manager/mysql-store';

const pool = mysql.createPool({ uri: process.env.DATABASE_URL });
await ensureSchema(pool); // creates `event_bus_outbox`/`event_bus_inbox` in the current database if missing
// custom prefix:
await ensureSchema(pool, 'my_prefix');
```

Pass the same `tablePrefix` option to `MySqlOutboxStore`/`MySqlInboxStore` if you used a custom one:
```ts
const outboxStore = new MySqlOutboxStore(pool, { tablePrefix: 'my_prefix' });
const inboxStore = new MySqlInboxStore(pool, { tablePrefix: 'my_prefix' });
```

## Outbox — publisher side
Write the event into the outbox table **inside the same transaction** as your business write, instead of calling `bus.publish()` directly.
```ts
import { MySqlOutboxStore } from '@event-bus-manager/mysql-store';
import { createEvent } from '@event-bus-manager/core';
import { UserCreatedV1 } from './events';

const outboxStore = new MySqlOutboxStore(pool);

async function createUser(input: { email: string }) {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [result] = await conn.query(
      'INSERT INTO users (email) VALUES (?)',
      [input.email],
    );
    await outboxStore.enqueue(
      conn, // the transaction connection, NOT the pool
      createEvent(UserCreatedV1, { userId: String(result.insertId), email: input.email }),
    );
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
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
import { MySqlInboxStore } from '@event-bus-manager/mysql-store';
import { SendWelcomeEmail } from './handlers/send-welcome-email';

const inboxStore = new MySqlInboxStore(pool);

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

Both `outbox` and `inbox` rows move through the same state machine as `@event-bus-manager/pg-store`:
```
outbox:  PENDING  → PROCESSING (leased) → SENT
inbox:  (no row)  → PROCESSING (leased) → PROCESSED
```
On failure, both go to `FAILED` but stay **reclaimable** — a `FAILED` row must remain claimable so the transport's own retry (`EventHandler.retry`) can redeliver the job and re-attempt `handle()`.

**Why explicit transactions instead of a single upsert statement**: MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` has no conditional `WHERE` clause, unlike Postgres's `ON CONFLICT DO UPDATE ... WHERE`. A "self-referencing `IF()` in the `SET` clause" trick was considered during design and rejected — MySQL evaluates `ON DUPLICATE KEY UPDATE` assignments left to right, and a later assignment's column reference sees the value an *earlier* assignment in the same statement just wrote, not the original row. Depending on assignment order, this silently breaks either the "reclaim a `FAILED` row" case or the "reclaim a stale `PROCESSING` row" case. Instead, both `claimBatch` (outbox) and `tryClaim` (inbox) open an explicit transaction, use `SELECT ... FOR UPDATE [SKIP LOCKED]` to lock candidate rows and decide reclaimability in SQL (avoiding app/DB clock skew), then issue a plain follow-up `INSERT`/`UPDATE` before committing. More verbose than a single statement, but each step is independently easy to verify correct — please don't "simplify" this back into a single upsert without re-deriving why it was avoided.

Locking uses `FOR UPDATE SKIP LOCKED` (outbox) / `FOR UPDATE` (inbox, single-row) inside a real transaction, so running multiple relay instances or consumer processes concurrently is safe — they never claim the same row twice.
