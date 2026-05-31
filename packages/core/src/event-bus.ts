import { EventBusLogger, defaultLogger } from "./logger";
import type { DomainEvent } from "./event";
import type { EventHandler } from "./event-handler";
import type { EventBusConfig, EventBusRole } from "./config";

export interface EventBus {
	start(): Promise<void>;
	stop(): Promise<void>;
	publish(event: DomainEvent): Promise<string[]>; // Returns job IDs
	subscribe(handler: EventHandler): void;
	/**
	 * Register a remote handler stub — declares the queue target without
	 * creating a local consumer. Use this in publisher-role services to
	 * tell the bus where to route events.
	 */
	registerRemoteHandler(
		handler: Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">,
	): void;
}

export abstract class CoreEventBus implements EventBus {
	protected handlers = new Map<string, EventHandler[]>();
	/** Remote handler stubs — queue targets only, no local consumer */
	protected remoteHandlers = new Map<
		string,
		Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">[]
	>();
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
	protected abstract _publishInternal(event: DomainEvent): Promise<string[]>;

	async publish(event: DomainEvent): Promise<string[]> {
		if (this.role === "consumer")
			throw new Error(
				`EventBus is configured as role="consumer" — publishing is disabled.`,
			);
		if (!this.started)
			throw new Error("Bus not started. Call start() before publishing.");

		this._log(`Publishing: ${event.name}`);
		return await this._publishInternal(event);
	}

	subscribe(handler: EventHandler): void {
		if (this.role === "publisher") {
			this.logger.warn(
				`EventBus role="publisher" — use registerRemoteHandler() instead of subscribe().`,
			);
			return;
		}

		// Filter by Event Name
		if (
			this.config.events !== "*" &&
			!this.config.events.includes(handler.eventName)
		) {
			return;
		}

		// Filter by Worker Name
		if (
			this.config.workers !== "*" &&
			!this.config.workers.includes(handler.handlerName)
		) {
			return;
		}

		const key = this._eventKey(handler.eventName, handler.eventVersion);
		if (!this.handlers.has(key)) this.handlers.set(key, []);
		this.handlers.get(key)!.push(handler);

		this._onHandlerSubscribed(key, handler);
	}

	registerRemoteHandler(
		handler: Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">,
	): void {
		const key = this._eventKey(handler.eventName, handler.eventVersion);
		if (!this.remoteHandlers.has(key)) this.remoteHandlers.set(key, []);
		const existing = this.remoteHandlers.get(key)!;
		if (!existing.some((h) => h.handlerName === handler.handlerName)) {
			existing.push(handler);
		}
		this._log(`Registered remote handler: ${key} → ${handler.handlerName}`);
	}

	protected _onHandlerSubscribed(_key: string, _handler: EventHandler): void {}

	protected async _executeHandlers(
		event: DomainEvent,
		handlers: EventHandler[],
	): Promise<void> {
		const results = await Promise.allSettled(
			handlers.map((h) => h.handle(event)),
		);
		results.forEach((result, i) => {
			if (result.status === "rejected") {
				this.logger.error(
					`Handler "${handlers[i]!.handlerName}" failed: ${result.reason}`,
				);
			}
		});
	}

	protected _eventKey(name: string, version: string): string {
		return `${name}@${version}`;
	}

	protected _log(message: string): void {
		if (this.config.debug) this.logger.info(message);
	}
}
