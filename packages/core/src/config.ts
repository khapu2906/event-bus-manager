export type EventBusRole = "both" | "publisher" | "consumer";
export type EventBusEvents = "*" | string[];
export type EventBusHandlers = "*" | string[];
export type EventBusWorkers = "*" | string[];

export interface EventBusAsyncConfig {
	maxRetries: number;
	retryDelay: number;
	eventTTL: string;
	archiveInterval: string;
	deleteArchivedInterval: string;
}

export interface EventBusConfig {
	role: EventBusRole;
	events: EventBusEvents;
	handlers: EventBusHandlers;
	workers: EventBusWorkers;
	async: EventBusAsyncConfig;
	debug?: boolean;
}

export function resolveCoreConfig(
	overrides?: Partial<EventBusConfig>,
): EventBusConfig {
	const resolveEvents = (): EventBusEvents => {
		const env = process.env.EVENT_BUS_EVENTS?.trim();
		if (!env || env === "*") return "*";
		return env
			.split(",")
			.map((e: string) => e.trim())
			.filter(Boolean);
	};

	const resolveHandlers = (): EventBusHandlers => {
		const env = process.env.EVENT_BUS_HANDLERS?.trim();
		if (!env || env === "*") return "*";
		return env
			.split(",")
			.map((h: string) => h.trim())
			.filter(Boolean);
	};

	const resolveWorkers = (): EventBusWorkers => {
		const env = process.env.EVENT_BUS_WORKERS?.trim();
		if (!env || env === "*") return "*";
		return env
			.split(",")
			.map((w: string) => w.trim())
			.filter(Boolean);
	};

	return {
		role: (process.env.EVENT_BUS_ROLE as any) || "both",
		events: resolveEvents(),
		handlers: resolveHandlers(),
		workers: resolveWorkers(),
		debug: process.env.EVENT_BUS_DEBUG === "true",
		async: {
			maxRetries: parseInt(process.env.EVENT_BUS_MAX_RETRIES || "3", 10),
			retryDelay: parseInt(process.env.EVENT_BUS_RETRY_DELAY || "5000", 10),
			eventTTL: process.env.EVENT_BUS_EVENT_TTL || "24 hours",
			archiveInterval: process.env.EVENT_BUS_ARCHIVE_INTERVAL || "1 hour",
			deleteArchivedInterval:
				process.env.EVENT_BUS_DELETE_ARCHIVED_INTERVAL || "7 days",
			...overrides?.async,
		},
		...overrides,
	};
}
