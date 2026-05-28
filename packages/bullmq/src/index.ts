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
	redis: {
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
	}

	async start(): Promise<void> {
		if (this.role !== "publisher") {
			for (const handlers of this.handlers.values()) {
				for (const handler of handlers) {
					this._registerWorker(handler);
				}
			}
		}
		this.started = true;
	}

	async stop(): Promise<void> {
		await Promise.all(this.workers.map((w) => w.close()));
		await Promise.all(Array.from(this.queues.values()).map((q) => q.close()));
		this.started = false;
	}

	protected async _publishInternal(event: DomainEvent): Promise<void> {
		const key = this._eventKey(event.name, event.version);
		const handlers = this.handlers.get(key) || [];
		await Promise.all(
			handlers.map((h) => {
				const q = this._getOrCreateQueue(this._queueName(h));
				return q.add(key, event);
			}),
		);
	}

	private _registerWorker(handler: EventHandler): void {
		const queueName = this._queueName(handler);
		const worker = new BullWorker(
			queueName,
			async (job: Job) => {
				await handler.handle(job.data);
			},
			{ connection: this.config.redis },
		);
		this.workers.push(worker);
	}

	private _getOrCreateQueue(name: string): Queue {
		if (!this.queues.has(name)) {
			this.queues.set(name, new Queue(name, { connection: this.config.redis }));
		}
		return this.queues.get(name)!;
	}

	private _queueName(handler: EventHandler): string {
		return `${handler.eventName}@${handler.eventVersion}--${handler.handlerName}`;
	}
}

registerEventBus("bullmq", (config: any) => new BullMQEventBus(config));
