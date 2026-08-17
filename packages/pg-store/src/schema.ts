import type { Pool } from "pg";

export const DEFAULT_SCHEMA = "event_bus";

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Schema names are spliced directly into SQL (Postgres doesn't allow
 * parameterizing identifiers via $1), so validate against a safe pattern
 * before ever using one in a query.
 */
export function assertValidSchema(schema: string): void {
	if (!IDENTIFIER_RE.test(schema)) {
		throw new Error(
			`Invalid Postgres schema name: "${schema}" — must match ${IDENTIFIER_RE}`,
		);
	}
}

export function outboxSchemaSql(schema: string = DEFAULT_SCHEMA): string {
	assertValidSchema(schema);
	return `
CREATE SCHEMA IF NOT EXISTS "${schema}";
CREATE TABLE IF NOT EXISTS "${schema}".outbox (
	id UUID PRIMARY KEY,
	event_name TEXT NOT NULL,
	event_version TEXT NOT NULL,
	payload JSONB NOT NULL,
	occurred_at TIMESTAMPTZ NOT NULL,
	status TEXT NOT NULL DEFAULT 'PENDING',
	claimed_by TEXT,
	claimed_at TIMESTAMPTZ,
	sent_at TIMESTAMPTZ,
	last_error TEXT
);
CREATE INDEX IF NOT EXISTS outbox_status_occurred_at_idx ON "${schema}".outbox (status, occurred_at);
`;
}

export function inboxSchemaSql(schema: string = DEFAULT_SCHEMA): string {
	assertValidSchema(schema);
	return `
CREATE SCHEMA IF NOT EXISTS "${schema}";
CREATE TABLE IF NOT EXISTS "${schema}".inbox (
	event_id UUID NOT NULL,
	handler_name TEXT NOT NULL,
	event_name TEXT NOT NULL,
	event_version TEXT NOT NULL,
	payload JSONB NOT NULL,
	status TEXT NOT NULL DEFAULT 'PROCESSING',
	claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	processed_at TIMESTAMPTZ,
	last_error TEXT,
	PRIMARY KEY (event_id, handler_name)
);
CREATE INDEX IF NOT EXISTS inbox_status_idx ON "${schema}".inbox (status);
`;
}

/** Creates the outbox/inbox tables (and the schema itself) if they don't already exist. */
export async function ensureSchema(pool: Pool, schema: string = DEFAULT_SCHEMA): Promise<void> {
	await pool.query(outboxSchemaSql(schema));
	await pool.query(inboxSchemaSql(schema));
}
