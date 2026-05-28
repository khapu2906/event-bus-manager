import type { EventDefinition } from './event-definition'

export interface DomainEvent<T = unknown> {
  name: string
  version: string
  payload: T
  occurredAt: Date
}

export function createEvent<T>(def: EventDefinition<T>, payload: T): DomainEvent<T> {
  return {
    name: def.name,
    version: def.version,
    payload,
    occurredAt: new Date(),
  }
}
