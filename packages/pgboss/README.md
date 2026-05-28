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

const bus = createEventBus('pgboss', {
  connectionString: 'postgresql://user:pass@localhost:5432/db'
});
```
