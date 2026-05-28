import { EventBusLogger, defaultLogger } from "./logger";
import { DomainEvent } from "./event";
import { EventHandler } from "./event-handler";
import { EventBusConfig, EventBusRole } from "./config";

export interface EventBus {
	start(): Promise<void>;
	stop(): Promise<void>;
	publish(event: DomainEvent): Promise<void>;
	subscribe(handler: EventHandler): void;
}

export abstract class CoreEventBus implements EventBus {
	protected handlers = new Map<string, EventHandler[]>();
	protected started = false;
	protected static instanceCount = 0;
	protected readonly instanceId: number;
	protected readonly role: EventBusRole;
	protected readonly logger: EventBusLogger;

	constructor(
		protected config: EventBusConfig,
		logger?: EventBusLogger,
	) {
		this.instanceId = (this.constructor as typeof CoreEventBus).instanceCount++;
		this.role = config.role || "both";
		this.logger = logger || defaultLogger;
	}

	abstract start(): Promise<void>;
	abstract stop(): Promise<void>;
	protected abstract _publishInternal(event: DomainEvent): Promise<void>;

	async publish(event: DomainEvent): Promise<void> {
		if (this.role === "consumer") throw new Error("Publisher disabled");
		if (!this.started) throw new Error("Bus not started");

		this._log(`Publishing: ${event.name}`);
		await this._publishInternal(event);
	}

	subscribe(handler: EventHandler): void {
		if (this.role === "publisher") return;

		// Filter by Event Name
		if (
			this.config.events !== "*" &&
			!this.config.events.includes(handler.eventName)
		) {
			this.logger.warn(`Event ${handler.eventName} not allowed`);
			return;
		}

		// Filter by Worker Name (EVENT_BUS_WORKERS filters by handlerName)
		if (
			this.config.workers !== "*" &&
			!this.config.workers.includes(handler.handlerName)
		) {
			this._log(`Handler ${handler.handlerName} skipped by EVENT_BUS_WORKERS`);
			return;
		}

		const key = this._eventKey(handler.eventName, handler.eventVersion);
		if (!this.handlers.has(key)) this.handlers.set(key, []);
		this.handlers.get(key)!.push(handler);

		this._onHandlerSubscribed(key, handler);
	}

	protected _onHandlerSubscribed(_key: string, _handler: EventHandler): void {}

	protected async _executeHandlers(
		event: DomainEvent,
		handlers: EventHandler[],
	): Promise<void> {
		await Promise.allSettled(handlers.map((h) => h.handle(event)));
	}

	protected _eventKey(name: string, version: string): string {
		return `${name}@${version}`;
	}

	protected _log(message: string): void {
		if (this.config.debug) this.logger.info(message);
	}
}
