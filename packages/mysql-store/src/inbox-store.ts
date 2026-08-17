import type { DomainEvent, InboxStore } from "@event-bus-manager/core";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { tableName } from "./schema";

interface ReclaimableRow extends RowDataPacket {
	reclaimable: number; // MySQL returns boolean expressions as 0/1
}

export interface MySqlInboxStoreOptions {
	/** Table name prefix, applied within the connection's current database. @default "event_bus" */
	tablePrefix?: string;
}

/**
 * MySQL-backed InboxStore, tracking idempotency per (eventId, handlerName).
 * See the note on `MySqlOutboxStore` for why this uses an explicit
 * transaction instead of a single `INSERT ... ON DUPLICATE KEY UPDATE`.
 */
export class MySqlInboxStore implements InboxStore {
	private readonly table: string;

	constructor(private readonly pool: Pool, options?: MySqlInboxStoreOptions) {
		this.table = tableName("inbox", options?.tablePrefix);
	}

	async tryClaim(event: DomainEvent, handlerName: string, leaseMs: number): Promise<boolean> {
		const conn = await this.pool.getConnection();
		try {
			await conn.beginTransaction();

			const [rows] = await conn.query<ReclaimableRow[]>(
				`SELECT
				   (status = 'FAILED' OR (status = 'PROCESSING' AND claimed_at < DATE_SUB(NOW(3), INTERVAL ? SECOND))) AS reclaimable
				 FROM ${this.table}
				 WHERE event_id = ? AND handler_name = ?
				 FOR UPDATE`,
				[Math.ceil(leaseMs / 1000), event.id, handlerName],
			);

			if (rows.length === 0) {
				await conn.query(
					`INSERT INTO ${this.table} (event_id, handler_name, event_name, event_version, payload, status, claimed_at)
					 VALUES (?, ?, ?, ?, ?, 'PROCESSING', NOW(3))`,
					[event.id, handlerName, event.name, event.version, JSON.stringify(event.payload)],
				);
				await conn.commit();
				return true;
			}

			if (!rows[0]!.reclaimable) {
				await conn.commit(); // releases the FOR UPDATE lock without changes
				return false;
			}

			await conn.query(
				`UPDATE ${this.table} SET status = 'PROCESSING', claimed_at = NOW(3) WHERE event_id = ? AND handler_name = ?`,
				[event.id, handlerName],
			);
			await conn.commit();
			return true;
		} catch (err) {
			await conn.rollback();
			throw err;
		} finally {
			conn.release();
		}
	}

	async markProcessed(eventId: string, handlerName: string): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'PROCESSED', processed_at = NOW(3) WHERE event_id = ? AND handler_name = ?`,
			[eventId, handlerName],
		);
	}

	async markFailed(eventId: string, handlerName: string, error: unknown): Promise<void> {
		await this.pool.query(
			`UPDATE ${this.table} SET status = 'FAILED', last_error = ? WHERE event_id = ? AND handler_name = ?`,
			[String(error), eventId, handlerName],
		);
	}
}
