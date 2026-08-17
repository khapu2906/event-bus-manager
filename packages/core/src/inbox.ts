import type { DomainEvent } from "./event";

/**
 * Idempotency store for the Inbox pattern — dedupes handler execution on
 * at-least-once redelivery. Tracked per (eventId, handlerName) since one
 * event can have multiple independent handlers.
 */
export interface InboxStore {
	/**
	 * Attempts to claim (event.id, handlerName) for processing.
	 * Returns false if already PROCESSED (permanent skip) or currently
	 * PROCESSING within an unexpired lease (another attempt is in flight).
	 * Returns true if the caller should proceed to run the handler — this
	 * covers first-time claims, retries of a FAILED attempt, and reclaiming
	 * a PROCESSING row whose lease has expired (crash recovery).
	 */
	tryClaim(event: DomainEvent, handlerName: string, leaseMs: number): Promise<boolean>;

	markProcessed(eventId: string, handlerName: string): Promise<void>;

	/** Should leave the record reclaimable — transport-level retry must still be able to re-attempt. */
	markFailed(eventId: string, handlerName: string, error: unknown): Promise<void>;
}
