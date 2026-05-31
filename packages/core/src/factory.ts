import type { EventBus } from "./event-bus";
import { InMemoryEventBus } from "./in-memory-event-bus";
import { resolveCoreConfig, EventBusConfig } from "./config";

export type EventBusFactory = (config: EventBusConfig) => EventBus;
const registry = new Map<string, EventBusFactory>();

// Register InMemory as default
registry.set("memory", (config) => new InMemoryEventBus(config));

export function registerEventBus(type: string, factory: EventBusFactory) {
	registry.set(type.toLowerCase(), factory);
}

export function createEventBus(overrides?: Partial<EventBusConfig>): EventBus {
	const config = { ...resolveCoreConfig(overrides), ...overrides };
	const factory = registry.get(config.type);
	if (!factory) {
		throw new Error(
			`EventBus type "${config.type}" not registered. Did you install and import the transport package?`,
		);
	}
	return factory(config);
}
