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

/** Whether a transport type is currently registered. */
export function hasEventBus(type: string): boolean {
	return registry.has(type.toLowerCase());
}

/** Lists all currently registered transport types. */
export function getRegisteredTypes(): string[] {
	return Array.from(registry.keys());
}

export function createEventBus(overrides?: Partial<EventBusConfig>): EventBus {
	// resolveCoreConfig already fully incorporates `overrides` (including a
	// proper nested merge for `async`) — do not re-spread `overrides` here,
	// it would clobber partial nested overrides with their raw, unmerged form.
	const config = resolveCoreConfig(overrides);
	const type = config.type.toLowerCase();
	const factory = registry.get(type);
	if (!factory) {
		throw new Error(
			`EventBus type "${config.type}" not registered. Registered types: [${getRegisteredTypes().join(", ")}]. Did you install and import the transport package?`,
		);
	}
	return factory(config);
}
