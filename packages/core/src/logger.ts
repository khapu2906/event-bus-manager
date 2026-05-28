import { CoreLogger } from "meo-meo-logger";

export interface EventBusLogger {
	info(message: string): void;
	warn(message: string): void;
	error(message: string): void;
}

export const defaultLogger: EventBusLogger = {
	info: (msg) => CoreLogger.info(`[event-bus] ${msg}`),
	warn: (msg) => CoreLogger.warn(`[event-bus] ${msg}`),
	error: (msg) => CoreLogger.error(`[event-bus] ${msg}`),
};
