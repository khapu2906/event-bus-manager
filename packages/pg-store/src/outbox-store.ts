import type { DomainEvent, OutboxStore } from "@event-bus-manager/core";
import type { Pool } from "pg";
import { DEFAULT_SCHEMA, assertValidSchema } from "./schema";

interface OutboxRow {
	id: string;
	event_name: string;
	event_version: string;
	payload: unknown;
	occurred_at: string | Date;
}

function rowToDomainEvent(row: OutboxRow): DomainEvent {
	return {
		id: row.id,
		name: row.event_name,
		version: row.event_version,
		payload: row.payload,
		occurredAt: new Date(row.occurred_at),
	};
}

export interface PgOutboxStoreOptions {
	/** Postgres schema holding the outbox table. @default "event_bus" */
	schema?: string;
}

/**
 * Postgres-backed OutboxStore. `enqueue` must be called with the caller's own
 * transaction client (whatever exposes `.query(sql, params)`) so the outbox
 * row commits atomically with the business write.
 */
export class PgOutboxStore implements OutboxStore {
	private readonly table: string;

	constructor(private readonly pool: Pool, options?: PgOutboxStoreOptions) {
		const schema = options?.schema ?? DEFAULT_SCHEMA;
		assertValidSchema(schema);
		this.table = `"${schema}".outbox`;
	}

	async enqueue(
		tx: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
		event: DomainEvent,
	): Promise<void> {
		await tx.query(
			`INSERT INTO ${this.table} (id, event_name, event_version, payload, occurred_at, status)
			 VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
			[event.id, event.name, event.version, JSON.stringify(event.payload), event.occurredAt],
		);
	}

	async claimBatch(relayId: string, limit: number, leaseMs: number): Promise<DomainEvent[]> {
		const { rows } = await this.pool.query<OutboxRow>(
			`UPDATE ${this.table}
			 SET status = 'PROCESSING', claimed_by = $1, claimed_at = now()
			 WHERE id IN (
			   SELECT id FROM ${this.table}
			   WHERE status = 'PENDING'
			      OR status = 'FAILED'
			      OR (status = 'PROCESSING' AND claimed_at < now() - $2::interval)
			   ORDER BY occurred_at ASC
			   LIMIT $3
			   FOR UPDATE SKIP LOCKED
			 )
			 RETURNING id, event_name, event_version, payload, occurred_at`,
			[relayId, `${leaseMs} milliseconds`, limit],
		);
		return rows.map(rowToDomainEvent);
	}

	async markSent(eventIds: string[]): Promise<void> {
		if (eventIds.length === 0) return;
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'SENT', sent_at = now() WHERE id = ANY($1)`,
			[eventIds],
		);
	}

	async markFailed(eventId: string, error: unknown): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'FAILED', last_error = $2 WHERE id = $1`,
			[eventId, String(error)],
		);
	}
}
