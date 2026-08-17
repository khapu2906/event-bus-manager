import { randomUUID } from "node:crypto";
import type { DomainEvent } from "./event";
import type { EventPublisher } from "./event-bus";

/**
 * Transactional outbox store — implementations persist events in the SAME
 * DB transaction as the caller's business write (via `enqueue`), and expose
 * a claim-based API for a separate relay to forward them to the real
 * transport reliably.
 */
export interface OutboxStore {
	/** `tx` is the caller's own transaction/client — implementation-specific. */
	enqueue(tx: unknown, event: DomainEvent): Promise<void>;

	/**
	 * Atomically claims up to `limit` unsent events for `relayId`, leasing
	 * them for `leaseMs`. Must be safe for multiple concurrent relay
	 * instances (e.g. `FOR UPDATE SKIP LOCKED`-style locking) and must also
	 * reclaim previously FAILED events and PROCESSING events whose lease
	 * has expired (crash recovery) — not just PENDING ones.
	 */
	claimBatch(relayId: string, limit: number, leaseMs: number): Promise<DomainEvent[]>;

	markSent(eventIds: string[]): Promise<void>;

	/** Should leave the record reclaimable — outbox failures are usually transient. */
	markFailed(eventId: string, error: unknown): Promise<void>;
}

export interface OutboxRelayOptions {
	pollingIntervalMs?: number;
	batchSize?: number;
	leaseMs?: number;
	relayId?: string;
}

const DEFAULT_POLLING_INTERVAL_MS = 500;
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_LEASE_MS = 30_000;

/**
 * Polls an `OutboxStore` and forwards claimed events to the real transport
 * (`publisher`). Never throws out of the poll loop — `claimBatch`/`publish`
 * failures are recorded (`markFailed`) or swallowed and retried next tick.
 */
export class OutboxRelay {
	private readonly relayId: string;
	private readonly pollingIntervalMs: number;
	private readonly batchSize: number;
	private readonly leaseMs: number;
	private running = false;
	private timer?: ReturnType<typeof setTimeout>;

	constructor(
		private readonly store: OutboxStore,
		private readonly publisher: EventPublisher,
		options?: OutboxRelayOptions,
	) {
		this.relayId = options?.relayId ?? randomUUID();
		this.pollingIntervalMs = options?.pollingIntervalMs ?? DEFAULT_POLLING_INTERVAL_MS;
		this.batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE;
		this.leaseMs = options?.leaseMs ?? DEFAULT_LEASE_MS;
	}

	async start(): Promise<void> {
		this.running = true;
		this._scheduleNext();
	}

	async stop(): Promise<void> {
		this.running = false;
		if (this.timer) clearTimeout(this.timer);
	}

	private _scheduleNext(): void {
		if (!this.running) return;
		this.timer = setTimeout(() => {
			void this._pollOnce();
		}, this.pollingIntervalMs);
	}

	private async _pollOnce(): Promise<void> {
		try {
			const events = await this.store.claimBatch(this.relayId, this.batchSize, this.leaseMs);
			const sentIds: string[] = [];
			for (const event of events) {
				try {
					await this.publisher.publish(event);
					sentIds.push(event.id);
				} catch (err) {
					await this.store.markFailed(event.id, err).catch(() => {});
				}
			}
			if (sentIds.length > 0) {
				await this.store.markSent(sentIds).catch(() => {});
			}
		} catch {
			// claimBatch itself failed (e.g. DB unavailable) — retry next tick.
		} finally {
			this._scheduleNext();
		}
	}
}
