import type { Pool } from "mysql2/promise";

export const DEFAULT_TABLE_PREFIX = "event_bus";

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Table prefixes are spliced directly into SQL (MySQL doesn't allow
 * parameterizing identifiers via `?`), so validate against a safe pattern
 * before ever using one in a query.
 */
export function assertValidIdentifier(name: string): void {
	if (!IDENTIFIER_RE.test(name)) {
		throw new Error(`Invalid MySQL identifier: "${name}" — must match ${IDENTIFIER_RE}`);
	}
}

/**
 * Tables are created in whatever database the connection is already using
 * (no `CREATE DATABASE`, no cross-database transaction) — MySQL has no
 * Postgres-style sub-database schema, and requiring a separate database
 * would mean the app's DB user needs a server-level `CREATE DATABASE`
 * privilege plus cross-database transaction support, which many managed
 * MySQL platforms restrict or don't support at all. A table prefix avoids
 * name collisions with your own tables without asking for any extra
 * privileges.
 */
export function tableName(name: "outbox" | "inbox", prefix: string = DEFAULT_TABLE_PREFIX): string {
	assertValidIdentifier(prefix);
	return `\`${prefix}_${name}\``;
}

export function outboxSchemaSql(prefix: string = DEFAULT_TABLE_PREFIX): string {
	const table = tableName("outbox", prefix);
	return `
CREATE TABLE IF NOT EXISTS ${table} (
	id CHAR(36) PRIMARY KEY,
	event_name VARCHAR(255) NOT NULL,
	event_version VARCHAR(64) NOT NULL,
	payload JSON NOT NULL,
	occurred_at DATETIME(3) NOT NULL,
	status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
	claimed_by VARCHAR(255),
	claimed_at DATETIME(3),
	sent_at DATETIME(3),
	last_error TEXT,
	INDEX ${prefix}_outbox_status_occurred_at_idx (status, occurred_at)
);
`;
}

export function inboxSchemaSql(prefix: string = DEFAULT_TABLE_PREFIX): string {
	const table = tableName("inbox", prefix);
	return `
CREATE TABLE IF NOT EXISTS ${table} (
	event_id CHAR(36) NOT NULL,
	handler_name VARCHAR(255) NOT NULL,
	event_name VARCHAR(255) NOT NULL,
	event_version VARCHAR(64) NOT NULL,
	payload JSON NOT NULL,
	status VARCHAR(16) NOT NULL DEFAULT 'PROCESSING',
	claimed_at DATETIME(3) NOT NULL,
	processed_at DATETIME(3),
	last_error TEXT,
	PRIMARY KEY (event_id, handler_name),
	INDEX ${prefix}_inbox_status_idx (status)
);
`;
}

/** Creates the outbox/inbox tables (in the connection's current database) if they don't already exist. */
export async function ensureSchema(pool: Pool, tablePrefix: string = DEFAULT_TABLE_PREFIX): Promise<void> {
	await pool.query(outboxSchemaSql(tablePrefix));
	await pool.query(inboxSchemaSql(tablePrefix));
}
