# Event Bus Manager

A powerful, multi-transport Event Bus system (InMemory, PgBoss, BullMQ) designed for distributed systems with a focus on **Worker Isolation**.

## 1. Project Structure

This project is organized as a monorepo containing the following packages:

| Package | Description | Drivers |
|---|---|---|
| `@event-bus-manager/core` | Core interfaces, filtering logic, and InMemory transport. | None |
| `@event-bus-manager/pgboss` | Postgres-backed transport using PgBoss. | `pg-boss` |
| `@event-bus-manager/bullmq` | Redis-backed transport using BullMQ. | `bullmq` |

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
| `EVENT_BUS_MAX_RETRIES` | `number` | Maximum retry attempts for failed jobs (default: `3`). |
| `EVENT_BUS_RETRY_DELAY` | `number` | Delay between retries in milliseconds (default: `5000`). |

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
  const bus = createEventBus(process.env.EVENT_BUS_TYPE || 'memory', {
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

## 5. Logging
The system uses `meo-meo-logger`. It automatically hooks into the global `CoreLogger`. Ensure you call `CoreLogger.configure()` in your main application before starting the bus.

## 6. Development
```bash
# Run tests
npm test
# Build packages
npm run build
```
