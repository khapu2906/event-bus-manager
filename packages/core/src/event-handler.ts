import type { DomainEvent } from "./event";
import type { EventBusAsyncConfig } from "./config";

export interface EventHandler<T = unknown> {
	eventName: string;
	eventVersion: string;
	handlerName: string; // This is Worker Identity
	/** Per-handler override of the bus-level retry policy (config.async). */
	retry?: Partial<Pick<EventBusAsyncConfig, "maxRetries" | "retryDelay">>;
	/** Per-handler override of the bus-level concurrency (config.concurrency). */
	concurrency?: number;
	handle(event: DomainEvent<T>): Promise<void>;
}
