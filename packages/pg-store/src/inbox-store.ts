import type { DomainEvent, InboxStore } from "@event-bus-manager/core";
import type { Pool } from "pg";
import { DEFAULT_SCHEMA, assertValidSchema } from "./schema";

export interface PgInboxStoreOptions {
	/** Postgres schema holding the inbox table. @default "event_bus" */
	schema?: string;
}

/** Postgres-backed InboxStore, tracking idempotency per (eventId, handlerName). */
export class PgInboxStore implements InboxStore {
	private readonly table: string;

	constructor(private readonly pool: Pool, options?: PgInboxStoreOptions) {
		const schema = options?.schema ?? DEFAULT_SCHEMA;
		assertValidSchema(schema);
		this.table = `"${schema}".inbox`;
	}

	async tryClaim(event: DomainEvent, handlerName: string, leaseMs: number): Promise<boolean> {
		const result = await this.pool.query(
			`INSERT INTO ${this.table} AS t (event_id, handler_name, event_name, event_version, payload, status, claimed_at)
			 VALUES ($1, $2, $3, $4, $5, 'PROCESSING', now())
			 ON CONFLICT (event_id, handler_name) DO UPDATE
			   SET status = 'PROCESSING', claimed_at = now()
			   WHERE t.status = 'FAILED'
			      OR (t.status = 'PROCESSING' AND t.claimed_at < now() - $6::interval)`,
			[event.id, handlerName, event.name, event.version, JSON.stringify(event.payload), `${leaseMs} milliseconds`],
		);
		return result.rowCount === 1;
	}

	async markProcessed(eventId: string, handlerName: string): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'PROCESSED', processed_at = now() WHERE event_id = $1 AND handler_name = $2`,
			[eventId, handlerName],
		);
	}

	async markFailed(eventId: string, handlerName: string, error: unknown): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'FAILED', last_error = $3 WHERE event_id = $1 AND handler_name = $2`,
			[eventId, handlerName, String(error)],
		);
	}
}
