import { EventBusLogger, defaultLogger } from "./logger";
import type { DomainEvent } from "./event";
import type { EventHandler } from "./event-handler";
import type { EventBusAsyncConfig, EventBusConfig, EventBusRole } from "./config";

const DEFAULT_INBOX_LEASE_MS = 60_000;

/** Slice of EventBus needed by publisher-role services. */
export interface EventPublisher {
	start(): Promise<void>;
	stop(): Promise<void>;
	publish(event: DomainEvent): Promise<string[]>; // Returns job IDs
	/**
	 * Register a remote handler stub — declares the queue target without
	 * creating a local consumer. Use this in publisher-role services to
	 * tell the bus where to route events.
	 */
	registerRemoteHandler(
		handler: Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">,
	): void;
}

/** Slice of EventBus needed by consumer-role services. */
export interface EventSubscriber {
	start(): Promise<void>;
	stop(): Promise<void>;
	subscribe(handler: EventHandler): void;
}

export interface EventBus extends EventPublisher, EventSubscriber {}

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

		// Always register the route — a publisher-role bus needs this to know
		// where to send jobs; a consumer/both-role bus gets it for free too
		// (harmless: _resolvePublishTargets dedupes remote entries that are
		// already present as local handlers).
		this.registerRemoteHandler(handler);

		if (this.role === "publisher") return;

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
			handlers.map((h) => this._guardedHandle(event, h)),
		);
		results.forEach((result, i) => {
			if (result.status === "rejected") {
				this.logger.error(
					`Handler "${handlers[i]!.handlerName}" failed: ${result.reason}`,
				);
			}
		});
	}

	/**
	 * Runs handler.handle(event), guarded by the bus-level InboxStore
	 * (config.inboxStore) if configured. Every transport must call this
	 * instead of handler.handle() directly so inbox dedup behaves
	 * identically everywhere. No-ops through to a plain call when no
	 * inboxStore is configured (opt-in, no behavior change by default).
	 */
	protected async _guardedHandle(event: DomainEvent, handler: EventHandler): Promise<void> {
		const inboxStore = this.config.inboxStore;
		if (!inboxStore) {
			await handler.handle(event);
			return;
		}
		const claimed = await inboxStore.tryClaim(event, handler.handlerName, DEFAULT_INBOX_LEASE_MS);
		if (!claimed) {
			this._log(
				`Skip ${handler.handlerName} for event ${event.id} — already processed or in-flight`,
			);
			return;
		}
		try {
			await handler.handle(event);
			await inboxStore.markProcessed(event.id, handler.handlerName);
		} catch (err) {
			await inboxStore.markFailed(event.id, handler.handlerName, err);
			throw err;
		}
	}

	protected _eventKey(name: string, version: string): string {
		return `${name}@${version}`;
	}

	protected _queueName(
		handler: Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">,
	): string {
		return `${this._eventKey(handler.eventName, handler.eventVersion)}--${handler.handlerName}`;
	}

	/**
	 * Merges local handlers with remote handler stubs for a given event key,
	 * deduped by handlerName (local wins). Every transport must use this to
	 * resolve publish targets so registerRemoteHandler() behaves identically
	 * regardless of backing transport.
	 */
	protected _resolvePublishTargets(
		key: string,
	): Pick<EventHandler, "eventName" | "eventVersion" | "handlerName">[] {
		const localHandlers = this.handlers.get(key) || [];
		const remoteHandlers = this.remoteHandlers.get(key) || [];
		return [
			...localHandlers,
			...remoteHandlers.filter(
				(r) => !localHandlers.some((l) => l.handlerName === r.handlerName),
			),
		];
	}

	protected _log(message: string): void {
		if (this.config.debug) this.logger.info(message);
	}

	/**
	 * Merges a handler's per-handler retry override on top of the bus-level
	 * default (config.async). Every transport must use this so the two-layer
	 * (global + per-handler) retry policy resolves identically everywhere.
	 */
	protected _resolveRetryPolicy(
		handler: Pick<EventHandler, "retry">,
	): Pick<EventBusAsyncConfig, "maxRetries" | "retryDelay"> {
		return {
			maxRetries: handler.retry?.maxRetries ?? this.config.async.maxRetries,
			retryDelay: handler.retry?.retryDelay ?? this.config.async.retryDelay,
		};
	}

	/**
	 * Merges a handler's per-handler concurrency override on top of the
	 * bus-level default (config.concurrency).
	 */
	protected _resolveConcurrency(handler: Pick<EventHandler, "concurrency">): number {
		return handler.concurrency ?? this.config.concurrency;
	}
}
