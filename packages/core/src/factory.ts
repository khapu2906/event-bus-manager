import { EventBus } from "./event-bus";
import { InMemoryEventBus } from "./in-memory-event-bus";
import { resolveCoreConfig } from "./config";

export type EventBusFactory = (config: any) => EventBus;
const registry = new Map<string, EventBusFactory>();

// Đăng ký InMemory mặc định
registry.set("memory", (config) => new InMemoryEventBus(config));

export function registerEventBus(type: string, factory: EventBusFactory) {
	registry.set(type.toLowerCase(), factory);
}

export function createEventBus(type: string, overrides?: any): EventBus {
	const config = resolveCoreConfig(overrides);
	const factory = registry.get(type.toLowerCase());
	if (!factory) {
		throw new Error(
			`EventBus type "${type}" not registered. Did you install and import the transport package?`,
		);
	}
	return factory(config);
}
