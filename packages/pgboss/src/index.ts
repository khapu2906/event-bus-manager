import {
	CoreEventBus,
	DomainEvent,
	EventHandler,
	EventBusConfig,
	EventBusLogger,
	registerEventBus,
} from "@event-bus-manager/core";
import { PgBoss, type Job } from "pg-boss";

export interface PgBossEventBusConfig extends EventBusConfig {
	connectionString?: string;
}

export class PgBossEventBus extends CoreEventBus {
	private boss: PgBoss;

	constructor(
		protected override config: PgBossEventBusConfig,
		logger?: EventBusLogger,
	) {
		super(config, logger);
		if (!config.connectionString) {
			throw new Error("PgBossEventBus requires config.connectionString");
		}
		this.boss = new PgBoss(config.connectionString);
	}

	async start(): Promise<void> {
		this._log(`PgBossEventBus instance #${this.instanceId} starting...`);
		await this.boss.start();

		if (this.role !== "publisher") {
			for (const handlers of this.handlers.values()) {
				for (const handler of handlers) {
					await this._registerWorker(handler);
				}
			}
		}
		this.started = true;
	}

	async stop(): Promise<void> {
		await this.boss.stop();
		this.started = false;
	}

	protected async _publishInternal(event: DomainEvent): Promise<string[]> {
		const key = this._eventKey(event.name, event.version);
		const allTargets = this._resolvePublishTargets(key);
		const results = await Promise.all(
			allTargets.map((h) =>
				this.boss.send(this._queueName(h), event, { id: event.id }),
			),
		);
		return results.filter((id): id is string => id !== null);
	}

	protected override _onHandlerSubscribed(
		_key: string,
		handler: EventHandler,
	): void {
		if (this.started) void this._registerWorker(handler);
	}

	private async _registerWorker(handler: EventHandler): Promise<void> {
		const queueName = this._queueName(handler);
		const policy = this._resolveRetryPolicy(handler);
		await this.boss.createQueue(queueName, {
			retryLimit: policy.maxRetries,
			// core's retryDelay is documented in milliseconds; pg-boss expects seconds.
			retryDelay: Math.round(policy.retryDelay / 1000),
			retryBackoff: false,
		});
		await this.boss.work(
			queueName,
			{ localConcurrency: this._resolveConcurrency(handler) },
			async (jobs: Array<Job<DomainEvent>>) => {
				for (const job of jobs) {
					try {
						const event: DomainEvent = {
							...job.data,
							occurredAt: new Date(job.data.occurredAt),
						};
						await this._guardedHandle(event, handler);
					} catch (error) {
						this.logger.error(`Handler ${handler.handlerName} failed: ${error}`);
						throw error;
					}
				}
			},
		);
	}
}

// Tự đăng ký vào Core Registry
registerEventBus(
	"pgboss",
	(config: PgBossEventBusConfig) => new PgBossEventBus(config),
);
