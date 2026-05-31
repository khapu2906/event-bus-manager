import type { EventDefinition } from "./event-definition";
import { randomUUID } from "node:crypto";

export interface DomainEvent<T = unknown> {
	id: string; // Unique event ID
	name: string;
	version: string;
	payload: T;
	occurredAt: Date;
}

export function createEvent<T>(
	def: EventDefinition<T>,
	payload: T,
	id?: string,
): DomainEvent<T> {
	return {
		id: id || randomUUID(),
		name: def.name,
		version: def.version,
		payload,
		occurredAt: new Date(),
	};
}
