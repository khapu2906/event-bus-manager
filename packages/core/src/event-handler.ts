import type { DomainEvent } from "./event";

export interface EventHandler<T = unknown> {
	eventName: string;
	eventVersion: string;
	handlerName: string; // Đây chính là Worker Identity
	handle(event: DomainEvent<T>): Promise<void>;
}
