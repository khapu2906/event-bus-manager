import {
	CoreEventBus,
	DomainEvent,
	EventHandler,
	EventBusConfig,
	EventBusLogger,
	registerEventBus,
} from "@event-bus-manager/core";
import { Queue, Worker as BullWorker, Job } from "bullmq";

export interface BullMQEventBusConfig extends EventBusConfig {
	redis?: {
		host: string;
		port: number;
	};
}

export class BullMQEventBus extends CoreEventBus {
	private readonly queues = new Map<string, Queue>();
	private readonly workers: BullWorker[] = [];

	constructor(
		protected override config: BullMQEventBusConfig,
		logger?: EventBusLogger,
	) {
		super(config, logger);
		if (!config.redis) {
			throw new Error("BullMQEventBus requires config.redis ({ host, port })");
		}
	}

	async start(): Promise<void> {
		this._log(
			`BullMQEventBus #${this.instanceId} starting (role: ${this.role})`,
		);
		if (this.role !== "publisher") {
			for (const handlers of this.handlers.values()) {
				for (const handler of handlers) {
					this._registerWorker(handler);
				}
			}
		}
		this.started = true;
		this._log(`BullMQEventBus #${this.instanceId} started`);
	}

	async stop(): Promise<void> {
		this._log(`BullMQEventBus #${this.instanceId} stopping`);
		await Promise.all(this.workers.map((w) => w.close()));
		await Promise.all(Array.from(this.queues.values()).map((q) => q.close()));
		this.started = false;
	}

	protected async _publishInternal(event: DomainEvent): Promise<void> {
		const key = this._eventKey(event.name, event.version);
		const handlers = this.handlers.get(key) || [];
		if (handlers.length === 0) {
			this._log(`No handlers for ${key}, skipping`);
			return;
		}
		await Promise.all(
			handlers.map((h) => {
				const q = this._getOrCreateQueue(this._queueName(h));
				this._log(`Queuing ${key} → ${this._queueName(h)}`);
				return q.add(key, event);
			}),
		);
	}

	protected override _onHandlerSubscribed(
		_key: string,
		handler: EventHandler,
	): void {
		if (this.started) this._registerWorker(handler);
	}

	private _registerWorker(handler: EventHandler): void {
		const queueName = this._queueName(handler);
		this._log(`Registering worker: ${queueName}`);
		const worker = new BullWorker(
			queueName,
			async (job: Job) => {
				try {
					await handler.handle(job.data);
				} catch (error) {
					this.logger.error(
						`Handler "${handler.handlerName}" failed for job ${job.id}: ${error}`,
					);
					throw error;
				}
			},
			{ connection: this.config.redis! },
		);
		worker.on("failed", (job, err) => {
			this.logger.error(
				`BullMQ worker "${handler.handlerName}" job ${job?.id ?? "unknown"} permanently failed: ${err}`,
			);
		});
		this.workers.push(worker);
	}

	private _getOrCreateQueue(name: string): Queue {
		if (!this.queues.has(name)) {
			this.queues.set(
				name,
				new Queue(name, { connection: this.config.redis! }),
			);
		}
		return this.queues.get(name)!;
	}

	private _queueName(handler: EventHandler): string {
		return `${handler.eventName}@${handler.eventVersion}--${handler.handlerName}`;
	}
}

registerEventBus(
	"bullmq",
	(config: BullMQEventBusConfig) => new BullMQEventBus(config),
);
