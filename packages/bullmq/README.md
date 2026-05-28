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

const bus = createEventBus('bullmq', {
  redis: { host: 'localhost', port: 6379 }
});
```
