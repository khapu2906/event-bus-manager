import { CoreEventBus } from "./event-bus";
import type { DomainEvent } from "./event";
import type { EventBusConfig } from "./config";

export class InMemoryEventBus extends CoreEventBus {
	constructor(config: EventBusConfig) {
		super(config);
	}

	async start(): Promise<void> {
		this._log(`InMemoryEventBus instance #${this.instanceId} starting...`);
		this.started = true;
	}

	async stop(): Promise<void> {
		this.started = false;
	}

	protected async _publishInternal(event: DomainEvent): Promise<string[]> {
		const key = this._eventKey(event.name, event.version);
		const handlers = this.handlers.get(key) || [];
		await this._executeHandlers(event, handlers);
		return [event.id]; // Return the generated event ID
	}
}
