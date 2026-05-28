# @event-bus-manager/core

The core package for the Event Bus Manager system. It provides interfaces, filtering logic, and the default InMemory transport.

## Features
- **Type-safe Event Definitions**: Use TypeScript generics for event payloads.
- **Worker Isolation**: Filter which handlers are registered based on environment variables.
- **Dynamic Registry**: Support for plugging in external transports (PgBoss, BullMQ).
- **Default Logger**: Seamlessly integrates with `meo-meo-logger` via `CoreLogger`.
- **InMemory Bus**: Zero-dependency transport included for local development and testing.

## Installation
```bash
npm install @event-bus-manager/core
```
