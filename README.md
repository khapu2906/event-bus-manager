# Event Bus Manager

A powerful, multi-transport Event Bus system (InMemory, PgBoss, BullMQ) designed for distributed systems with a focus on **Worker Isolation**.

## 1. Project Structure

This project is organized as a monorepo containing the following packages:

| Package | Description | Drivers |
|---|---|---|
| `@event-bus-manager/core` | Core interfaces, filtering logic, and InMemory transport. | None |
| `@event-bus-manager/pgboss` | Postgres-backed transport using PgBoss. | `pg-boss` |
| `@event-bus-manager/bullmq` | Redis-backed transport using BullMQ. | `bullmq` |
| `@event-bus-manager/pg-store` | Postgres-backed Outbox/Inbox stores (reliability, opt-in). | `pg` |
| `@event-bus-manager/mysql-store` | MySQL-backed Outbox/Inbox stores (reliability, opt-in). | `mysql2` |

## 2. Core Concepts

### Worker Isolation (Filtering)
Worker Isolation allows you to distribute load by restricting which handlers run on which process. You can filter at two levels:

1.  **Event Level (`EVENT_BUS_EVENTS`)**: Restrict the bus to only listen to specific event names.
2.  **Worker Level (`EVENT_BUS_WORKERS`)**: Restrict the bus to only register specific Handler classes. **In this system, a Worker is identified by its `handlerName`.**

### Environment Variables

| Variable | Values | Description |
|---|---|---|
| `EVENT_BUS_TYPE` | `memory` \| `pgboss` \| `bullmq` | The transport infrastructure to use. |
| `EVENT_BUS_ROLE` | `publisher` \| `consumer` \| `both` | Role of the current node (default: `both`). |
| `EVENT_BUS_WORKERS` | `HandlerA,HandlerB` | List of Handler class names allowed to run. Use `*` for all. |
| `EVENT_BUS_EVENTS` | `event1,event2` | List of Event names allowed to be subscribed. Use `*` for all. |
| `EVENT_BUS_DEBUG` | `true` \| `false` | Enable detailed logging. |
| `EVENT_BUS_CONCURRENCY` | `number` | Bus-level default: number of jobs processed in parallel per handler (default: `1`). Overridable per handler via `EventHandler.concurrency`. |
| `EVENT_BUS_MAX_RETRIES` | `number` | Bus-level default: number of retries *after* the first attempt for failed jobs (default: `3`). Overridable per handler via `EventHandler.retry.maxRetries`. |
| `EVENT_BUS_RETRY_DELAY` | `number` | Bus-level default: delay between retries, in **milliseconds** (default: `5000`). Overridable per handler via `EventHandler.retry.retryDelay`. |

## 3. Integration Guide

### Step 1: Define an Event
Use `defineEvent` to create type-safe event definitions.

```ts
import { defineEvent } from '@event-bus-manager/core';

export const UserCreatedV1 = defineEvent<{ userId: string; email: string }>(
  'user.created', 
  'v1'
);
```

### Step 2: Implement a Handler
Implement the `EventHandler` interface. The `handlerName` is used for filtering.

```ts
import { EventHandler, DomainEvent, PayloadOf } from '@event-bus-manager/core';
import { UserCreatedV1 } from './events';

type Payload = PayloadOf<typeof UserCreatedV1>;

export class SendWelcomeEmail implements EventHandler<Payload> {
  readonly eventName = UserCreatedV1.name;
  readonly eventVersion = UserCreatedV1.version;
  readonly handlerName = "SendWelcomeEmail"; // Identity for EVENT_BUS_WORKERS

  // Optional: override the bus-level retry policy (EVENT_BUS_MAX_RETRIES /
  // EVENT_BUS_RETRY_DELAY) for this handler's queue only.
  readonly retry = { maxRetries: 5, retryDelay: 10_000 };

  // Optional: override EVENT_BUS_CONCURRENCY — how many jobs this handler
  // processes in parallel.
  readonly concurrency = 5;

  async handle(event: DomainEvent<Payload>) {
    const { email } = event.payload;
    console.log(`Sending welcome email to ${email}`);
  }
}
```

### Step 3: Initialize the Bus
Use the factory to create a bus instance based on environment configuration.

```ts
import { createEventBus, createEvent } from '@event-bus-manager/core';
import '@event-bus-manager/pgboss'; // Required for registry side-effects

async function bootstrap() {
  const bus = createEventBus({
    type: process.env.EVENT_BUS_TYPE || 'memory',
    connectionString: process.env.DATABASE_URL, // Required for PgBoss
    debug: true
  });

  await bus.start();

  // Subscribe your handlers
  bus.subscribe(new SendWelcomeEmail());

  // Publish an event
  await bus.publish(createEvent(UserCreatedV1, { 
    userId: '123', 
    email: 'hello@example.com' 
  }));
}
```

## 4. Deployment Scenarios

**Scenario A: All-in-one process**
```bash
EVENT_BUS_WORKERS=*
```

**Scenario B: Dedicated Email Worker**
```bash
EVENT_BUS_WORKERS=SendWelcomeEmail
```

**Scenario C: Publisher-only gateway (`EVENT_BUS_ROLE=publisher`)**
Call `bus.subscribe(new SendWelcomeEmail())` exactly like the other scenarios — no role-specific code needed. `subscribe()` always registers the route so the bus knows which queue to publish to; when `role=publisher` it just skips actually running the handler locally. You still need the handler *class* importable in this process (even though it never runs) purely to get its `eventName`/`eventVersion`/`handlerName`. If you don't want to import the handler class at all, call `bus.registerRemoteHandler({ eventName, eventVersion, handlerName })` directly with just those three fields instead.

## 5. Reliability: Outbox & Inbox

Both patterns are **opt-in** — if unused, behavior is unchanged.

**Outbox** (publisher side) fixes the dual-write problem: `bus.publish()` runs independently of your own DB transaction, so a crash between your business commit and the publish call loses the event. Instead, write the event into an outbox table *inside* your own transaction via `OutboxStore.enqueue(tx, event)`, and run a separate `OutboxRelay` (from `@event-bus-manager/core`) that polls the store and forwards to the real transport.

**Inbox** (consumer side) fixes duplicate processing on at-least-once redelivery: pass an `InboxStore` as `inboxStore` in the bus config, and every handler subscribed to that bus is automatically guarded — a redelivered event that already completed successfully for a given handler is skipped, with no changes needed in `handle()`.

`core` only defines the `OutboxStore`/`InboxStore` interfaces (zero DB dependency, same principle as the transport plugins). A ready-to-use Postgres implementation — `PgOutboxStore`, `PgInboxStore`, plus schema/migration helpers — ships in `@event-bus-manager/pg-store`. See that package's README for full usage examples.

## 6. Logging
The system uses `meo-meo-logger`. It automatically hooks into the global `CoreLogger`. Ensure you call `CoreLogger.configure()` in your main application before starting the bus.

## 7. Development
There is no root workspace — each package under `packages/` is installed and built independently (no `workspace:*` linking). Run commands from inside the package you're working on:
```bash
cd packages/core
npm install
npm test          # vitest — core only, other packages have no test suite yet
npm run typecheck
npm run build      # tsup — same script name in every package (bullmq, pgboss, pg-store)
```
