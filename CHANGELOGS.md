# Changelog

All notable changes across the `event-bus-manager` packages. Grouped by package; format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## 2026-08-17

### `@event-bus-manager/core` 0.2.2

**Added**
- `EventPublisher`/`EventSubscriber` interfaces — `EventBus` now extends both, so a publisher-only service can depend on just `EventPublisher`.
- `hasEventBus(type)` / `getRegisteredTypes()` on the transport registry (diagnostics for the "not registered" error path).
- Per-handler retry override: `EventHandler.retry` merges over the bus-level `EVENT_BUS_MAX_RETRIES`/`EVENT_BUS_RETRY_DELAY` defaults via `_resolveRetryPolicy`.
- Per-handler concurrency override: `EventHandler.concurrency` merges over the bus-level `EVENT_BUS_CONCURRENCY` default via `_resolveConcurrency`.
- Outbox pattern: `OutboxStore` interface + `OutboxRelay` (generic poll/claim/publish/mark loop).
- Inbox pattern: `InboxStore` interface + internal `_guardedHandle` — opt-in via `EventBusConfig.inboxStore`, dedupes handler execution on at-least-once redelivery.
- Shared `_resolvePublishTargets`/`_queueName` on `CoreEventBus`, used by every transport instead of each reimplementing them.

**Changed**
- `subscribe(handler)` now always registers the handler's route (same as `registerRemoteHandler`) regardless of `role`, then additionally does local registration unless `role === "publisher"`. Previously, calling `subscribe()` on a `role: "publisher"` bus just logged a warning and silently dropped the handler — callers had to branch their own code between `subscribe()` and `registerRemoteHandler()` depending on role. Same call now works uniformly in any role.

**Fixed**
- `createEventBus` didn't lowercase `type` before registry lookup, so mixed-case overrides (e.g. `type: 'BullMQ'`) failed with "not registered" even when the transport was installed.
- `createEventBus`'s config merge re-spread raw `overrides` after `resolveCoreConfig` had already merged them, clobbering partial `async` overrides (losing bus-level defaults for any field not explicitly overridden).
- The fix above initially dropped transport-specific extra fields (`redis`, `connectionString`, ...) entirely — corrected by moving the spread inside `resolveCoreConfig` itself.
- `PgBossEventBus` didn't merge `remoteHandlers` into publish targets the way `BullMQEventBus` did — an LSP violation where `registerRemoteHandler()` silently did nothing on the pg-boss transport. Fixed by centralizing the merge in `CoreEventBus`.

**Removed**
- Unused `EventBusHandlers` type / `config.handlers` field — resolved from env but never read by any filtering logic.

**Breaking**
- `EventBusConfig.concurrency` is a required field (not optional). Anyone constructing an `EventBusConfig`-shaped object by hand instead of going through `resolveCoreConfig`/`createEventBus` needs to supply it.
- `config.handlers`/`EventBusHandlers` removed (see above).

### `@event-bus-manager/bullmq` 0.2.2
Requires `@event-bus-manager/core` ^0.2.1.

**Added**
- Retry/concurrency wired into real `Queue`/`Worker` options: `attempts` (= `maxRetries + 1`), `backoff: { type: 'fixed', delay: retryDelay }`, `concurrency`.
- Inbox guard (`_guardedHandle`) wired into the job processor.

**Fixed**
- Removed the duplicated `_queueName` method and manual `remoteHandlers` merge (now inherited from `core`), dropping the unsafe `(this as any)` casts that were needed to work around it.

### `@event-bus-manager/pgboss` 0.2.2
Requires `@event-bus-manager/core` ^0.2.1 (previously pinned to an exact version — switched to a caret range for consistency with `bullmq`).

**Added**
- Retry/concurrency wired into `createQueue`/`work()` options: `retryLimit`, `retryDelay` (ms → seconds), `retryBackoff: false`, `localConcurrency`.
- Inbox guard (`_guardedHandle`) wired into the `work()` callback.

**Fixed**
- Same `_queueName`/`remoteHandlers` fix as bullmq — this was the transport where the LSP bug was actually observable (`registerRemoteHandler` was a no-op here before the fix).

### `@event-bus-manager/pg-store` 0.1.1
Requires `@event-bus-manager/core` ^0.2.1.

**Added**
- `PgOutboxStore` / `PgInboxStore` — Postgres implementations of `OutboxStore`/`InboxStore`.
- Tables live in a dedicated Postgres schema (default `event_bus`, configurable via `{ schema }` on both stores and `ensureSchema(pool, schema)`) instead of `public` — same convention `pg-boss` itself uses, avoids colliding with application tables named `outbox`/`inbox`.
- `ensureSchema(pool, schema?)` + SQL generator functions (`outboxSchemaSql`/`inboxSchemaSql`) for the `outbox`/`inbox` tables.
- Claim queries correctly reclaim `FAILED` rows (not just stale `PROCESSING` ones) so a failed attempt doesn't permanently block a later retry.
- Schema name is validated against a safe identifier pattern before being spliced into SQL (schema names can't be parameterized via `$1` in Postgres DDL).

### `@event-bus-manager/mysql-store` 0.1.1
Requires `@event-bus-manager/core` ^0.2.1. Requires MySQL 8.0+ (`SELECT ... FOR UPDATE SKIP LOCKED`).

**Added**
- `MySqlOutboxStore` / `MySqlInboxStore` — MySQL implementations of `OutboxStore`/`InboxStore`, same claim/reclaim state machine as `pg-store` (`FAILED` rows stay reclaimable, `SENT`/`PROCESSED` are terminal).
- Tables are created in the connection's *current* database with a name prefix (default `event_bus`, configurable via `{ tablePrefix }` on both stores and `ensureSchema(pool, tablePrefix)`) rather than a separate database — an earlier draft created a dedicated database (mirroring `pg-store`'s dedicated schema), but that requires a server-level `CREATE DATABASE` privilege plus cross-database transaction support that many managed MySQL platforms (e.g. PlanetScale) restrict or don't support; a table prefix avoids the collision risk without asking for extra privileges.
- Claiming uses an explicit transaction (`getConnection()`/`beginTransaction()`/`SELECT ... FOR UPDATE [SKIP LOCKED]`/`commit()`) rather than a single upsert statement — MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` has no conditional `WHERE`, and a single-statement "self-referencing `IF()`" trick was evaluated and rejected during design (assignment order changes which values later expressions in the same statement see, silently breaking one of the two reclaim cases). See the package README's "How claiming works" for the full reasoning.

### Docs

- Fixed a stale `createEventBus('type', config)` two-argument example (real signature takes one config object) in the root, `bullmq`, and `pgboss` READMEs.
- Documented retry/concurrency/Outbox/Inbox wherever they apply; corrected the root README's "Development" section (there is no root workspace/`package.json` — commands must be run per-package).

### Package metadata (all 5 packages)

- Added `repository` (with `directory`), `homepage`, and `bugs` fields to every `package.json`, pointing at `github.com/khapu2906/event-bus-manager`.
