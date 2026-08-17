import type { InboxStore } from "./inbox";

export type EventBusRole = "both" | "publisher" | "consumer";
export type EventBusEvents = "*" | string[];
export type EventBusWorkers = "*" | string[];

export interface EventBusAsyncConfig {
	maxRetries: number;
	retryDelay: number;
	eventTTL: string;
	archiveInterval: string;
	deleteArchivedInterval: string;
}

export type EventBusType = string;

export interface EventBusConfig {
	type: EventBusType;
	role: EventBusRole;
	events: EventBusEvents;
	workers: EventBusWorkers;
	async: EventBusAsyncConfig;
	concurrency: number;
	/** Opt-in Inbox idempotency store — no behavior change if unset. */
	inboxStore?: InboxStore;
	debug?: boolean;
	// Allow transport-specific extra fields (e.g. redis, connectionString)
	[key: string]: unknown;
}

/** Parses a comma-separated env var into a list, or "*" when unset/wildcard. */
function parseListEnv(name: string): "*" | string[] {
	const env = process.env[name]?.trim();
	if (!env || env === "*") return "*";
	return env
		.split(",")
		.map((v) => v.trim())
		.filter(Boolean);
}

export function resolveCoreConfig(
	overrides?: Partial<EventBusConfig>,
): EventBusConfig {
	const resolveType = (): EventBusType => {
		return process.env.EVENT_BUS_TYPE?.toLowerCase() || "memory";
	};

	return {
		// Spread first so transport-specific extra fields (redis, connectionString, ...)
		// pass through untouched; the explicit keys below always win over raw overrides.
		...overrides,
		type: overrides?.type ?? resolveType(),
		role:
			overrides?.role ??
			((process.env.EVENT_BUS_ROLE as EventBusRole) || "both"),
		events: overrides?.events ?? parseListEnv("EVENT_BUS_EVENTS"),
		workers: overrides?.workers ?? parseListEnv("EVENT_BUS_WORKERS"),
		concurrency:
			overrides?.concurrency ??
			parseInt(process.env.EVENT_BUS_CONCURRENCY || "1", 10),
		debug: overrides?.debug ?? process.env.EVENT_BUS_DEBUG === "true",
		async: {
			maxRetries: parseInt(process.env.EVENT_BUS_MAX_RETRIES || "3", 10),
			retryDelay: parseInt(process.env.EVENT_BUS_RETRY_DELAY || "5000", 10),
			eventTTL: process.env.EVENT_BUS_EVENT_TTL || "24 hours",
			archiveInterval: process.env.EVENT_BUS_ARCHIVE_INTERVAL || "1 hour",
			deleteArchivedInterval:
				process.env.EVENT_BUS_DELETE_ARCHIVED_INTERVAL || "7 days",
			...overrides?.async,
		},
	};
}
