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

	protected async _publishInternal(event: DomainEvent): Promise<void> {
		const key = this._eventKey(event.name, event.version);
		const handlers = this.handlers.get(key) || [];
		await Promise.all(
			handlers.map((h) => this.boss.send(this._queueName(h), event)),
		);
	}

	protected override _onHandlerSubscribed(
		_key: string,
		handler: EventHandler,
	): void {
		if (this.started) void this._registerWorker(handler);
	}

	private async _registerWorker(handler: EventHandler): Promise<void> {
		const queueName = this._queueName(handler);
		await this.boss.createQueue(queueName);
		await this.boss.work(queueName, async (jobs: Array<Job<DomainEvent>>) => {
			for (const job of jobs) {
				try {
					await handler.handle(job.data);
				} catch (error) {
					this.logger.error(`Handler ${handler.handlerName} failed: ${error}`);
					throw error;
				}
			}
		});
	}

	private _queueName(handler: EventHandler): string {
		return `${handler.eventName}@${handler.eventVersion}--${handler.handlerName}`;
	}
}

// Tự đăng ký vào Core Registry
registerEventBus(
	"pgboss",
	(config: PgBossEventBusConfig) => new PgBossEventBus(config),
);
