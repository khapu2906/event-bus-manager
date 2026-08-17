# @event-bus-manager/core

The core package for the Event Bus Manager system. It provides the transport-agnostic interfaces, filtering/role logic, the default InMemory transport, and the (opt-in) Outbox/Inbox abstractions. It has zero dependency on any specific broker or database — transports (`bullmq`, `pgboss`) and stores (`pg-store`) are separate packages that plug into it.

## Features
- **Type-safe Event Definitions**: `defineEvent`/`createEvent`/`PayloadOf` for typed event payloads.
- **Worker Isolation**: Filter which handlers/events are registered per process via `EVENT_BUS_WORKERS`/`EVENT_BUS_EVENTS`, and enforce publisher/consumer/both `role`.
- **Interface segregation**: `EventBus` splits into `EventPublisher` (`publish`, `registerRemoteHandler`) and `EventSubscriber` (`subscribe`) — a publisher-only service can depend on just `EventPublisher`.
- **Retry & concurrency, with per-handler override**: bus-level defaults (`EVENT_BUS_MAX_RETRIES`/`EVENT_BUS_RETRY_DELAY`/`EVENT_BUS_CONCURRENCY`) overridable per handler via `EventHandler.retry`/`EventHandler.concurrency`. Wired into both `bullmq` and `pgboss` transports.
- **Dynamic transport registry**: `registerEventBus`/`createEventBus`/`hasEventBus`/`getRegisteredTypes` — plug in external transports (`pgboss`, `bullmq`, or your own) without `core` knowing about them.
- **Outbox & Inbox (opt-in reliability)**: `OutboxStore`/`OutboxRelay` for atomic "write DB + emit event" (dual-write problem); `InboxStore` + the bus's internal guard for idempotent handler execution on at-least-once redelivery. `core` only defines the interfaces — a ready Postgres implementation ships in `@event-bus-manager/pg-store`.
- **Default Logger**: Seamlessly integrates with `meo-meo-logger` via `CoreLogger`, injectable via the `EventBusLogger` interface.
- **InMemory Bus**: Zero-dependency transport included for local development and testing.

## Installation
```bash
npm install @event-bus-manager/core
```

Install a transport alongside it — see `@event-bus-manager/bullmq` or `@event-bus-manager/pgboss` — and import it once for its registry side-effect before calling `createEventBus()`.

## Key exports
| Export | Purpose |
|---|---|
| `defineEvent`, `createEvent`, `PayloadOf` | Type-safe event definitions and instances. |
| `EventBus`, `EventPublisher`, `EventSubscriber`, `CoreEventBus` | Core contracts; extend `CoreEventBus` to build a new transport. |
| `EventHandler` | Handler contract — `handlerName`, optional `retry`/`concurrency` overrides. |
| `createEventBus`, `registerEventBus`, `hasEventBus`, `getRegisteredTypes` | Transport factory/registry. |
| `resolveCoreConfig`, `EventBusConfig` | Env-driven config resolution; accepts transport-specific extra fields (`redis`, `connectionString`, `inboxStore`, ...). |
| `OutboxStore`, `OutboxRelay` | Outbox pattern abstraction + generic relay loop. |
| `InboxStore` | Inbox pattern abstraction (idempotency store), consumed automatically once passed as `inboxStore` in config. |
| `InMemoryEventBus` | Built-in `memory` transport, registered by default. |

See the root [README](../../README.md) for the full integration guide, environment variables, and deployment scenarios.
