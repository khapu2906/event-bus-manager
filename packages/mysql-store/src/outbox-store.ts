import type { DomainEvent, OutboxStore } from "@event-bus-manager/core";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { tableName } from "./schema";

interface OutboxRow extends RowDataPacket {
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
		// mysql2 already parses JSON columns into JS values.
		payload: row.payload,
		occurredAt: new Date(row.occurred_at),
	};
}

export interface MySqlOutboxStoreOptions {
	/** Table name prefix, applied within the connection's current database. @default "event_bus" */
	tablePrefix?: string;
}

/**
 * MySQL-backed OutboxStore. `enqueue` must be called with the caller's own
 * transaction connection (whatever exposes `.query(sql, params)`) so the
 * outbox row commits atomically with the business write.
 *
 * `claimBatch`/`tryClaim`-style methods here use an explicit
 * getConnection()/beginTransaction()/commit() sequence rather than a single
 * upsert statement — MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` has no
 * conditional WHERE clause, and a "self-referencing IF() in SET" trick was
 * evaluated and rejected during design (assignment order in the same
 * statement changes which column values are visible to later expressions,
 * silently breaking one of the two reclaim cases depending on ordering).
 * The explicit-transaction version is more verbose but each step is
 * independently easy to verify correct — do not "simplify" this back into
 * a single upsert without re-deriving that trade-off.
 */
export class MySqlOutboxStore implements OutboxStore {
	private readonly table: string;

	constructor(private readonly pool: Pool, options?: MySqlOutboxStoreOptions) {
		this.table = tableName("outbox", options?.tablePrefix);
	}

	async enqueue(
		tx: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
		event: DomainEvent,
	): Promise<void> {
		await tx.query(
			`INSERT INTO ${this.table} (id, event_name, event_version, payload, occurred_at, status)
			 VALUES (?, ?, ?, ?, ?, 'PENDING')`,
			[event.id, event.name, event.version, JSON.stringify(event.payload), event.occurredAt],
		);
	}

	async claimBatch(relayId: string, limit: number, leaseMs: number): Promise<DomainEvent[]> {
		const conn = await this.pool.getConnection();
		try {
			await conn.beginTransaction();

			const [rows] = await conn.query<OutboxRow[]>(
				`SELECT id, event_name, event_version, payload, occurred_at FROM ${this.table}
				 WHERE status = 'PENDING'
				    OR status = 'FAILED'
				    OR (status = 'PROCESSING' AND claimed_at < DATE_SUB(NOW(3), INTERVAL ? SECOND))
				 ORDER BY occurred_at ASC
				 LIMIT ?
				 FOR UPDATE SKIP LOCKED`,
				[Math.ceil(leaseMs / 1000), limit],
			);

			if (rows.length > 0) {
				const ids = rows.map((r) => r.id);
				await conn.query(
					`UPDATE ${this.table} SET status = 'PROCESSING', claimed_by = ?, claimed_at = NOW(3) WHERE id IN (?)`,
					[relayId, ids],
				);
			}

			await conn.commit();
			return rows.map(rowToDomainEvent);
		} catch (err) {
			await conn.rollback();
			throw err;
		} finally {
			conn.release();
		}
	}

	async markSent(eventIds: string[]): Promise<void> {
		if (eventIds.length === 0) return;
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'SENT', sent_at = NOW(3) WHERE id IN (?)`,
			[eventIds],
		);
	}

	async markFailed(eventId: string, error: unknown): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'FAILED', last_error = ? WHERE id = ?`,
			[String(error), eventId],
		);
	}
}
