import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
	CoreEventBus,
	resolveCoreConfig,
	EventHandler,
	InMemoryEventBus,
	createEvent,
	createEventBus,
	defineEvent,
	DomainEvent,
	hasEventBus,
	getRegisteredTypes,
	OutboxRelay,
	OutboxStore,
	InboxStore,
	EventPublisher,
} from "../src";

// ---------------------------------------------------------------------------
// Test double
// ---------------------------------------------------------------------------

class TestBus extends CoreEventBus {
	async start() {
		this.started = true;
	}
	async stop() {
		this.started = false;
	}
	protected async _publishInternal(_event: DomainEvent): Promise<void> {}
	public getRegisteredHandlers(key: string) {
		return this.handlers.get(key);
	}
	public getRemoteHandlers(key: string) {
		return this.remoteHandlers.get(key);
	}
	public getPublishTargets(key: string) {
		return this._resolvePublishTargets(key);
	}
	public getRetryPolicy(handler: Pick<EventHandler, "retry">) {
		return this._resolveRetryPolicy(handler);
	}
	public getConcurrency(handler: Pick<EventHandler, "concurrency">) {
		return this._resolveConcurrency(handler);
	}
	public guardedHandle(event: DomainEvent, handler: EventHandler) {
		return this._guardedHandle(event, handler);
	}
}

const mockHandler = (name: string, event = "test.event"): EventHandler => ({
	eventName: event,
	eventVersion: "v1",
	handlerName: name,
	handle: vi.fn(),
});

const testEventDef = defineEvent<{ id: number }>("test.event", "v1");

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

describe("Filtering", () => {
	beforeEach(() => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
	});

	it("allows all handlers when no filter set", () => {
		const bus = new TestBus(resolveCoreConfig());
		bus.subscribe(mockHandler("H1"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toHaveLength(1);
	});

	it("filters by EVENT_BUS_WORKERS", () => {
		vi.stubEnv("EVENT_BUS_WORKERS", "SendEmail,SyncData");
		const bus = new TestBus(resolveCoreConfig());

		bus.subscribe(mockHandler("SendEmail"));
		bus.subscribe(mockHandler("SyncData"));
		bus.subscribe(mockHandler("OtherHandler"));

		const registered = bus.getRegisteredHandlers("test.event@v1");
		expect(registered).toHaveLength(2);
		expect(registered![0].handlerName).toBe("SendEmail");
		expect(registered![1].handlerName).toBe("SyncData");
	});

	it("blocks all when EVENT_BUS_WORKERS has no match", () => {
		vi.stubEnv("EVENT_BUS_WORKERS", "NoneMatch");
		const bus = new TestBus(resolveCoreConfig());
		bus.subscribe(mockHandler("SomeHandler"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toBeUndefined();
	});

	it("filters by EVENT_BUS_EVENTS", () => {
		vi.stubEnv("EVENT_BUS_EVENTS", "allowed.event");
		const bus = new TestBus(resolveCoreConfig());

		bus.subscribe(mockHandler("H1", "allowed.event"));
		bus.subscribe(mockHandler("H2", "blocked.event"));

		expect(bus.getRegisteredHandlers("allowed.event@v1")).toHaveLength(1);
		expect(bus.getRegisteredHandlers("blocked.event@v1")).toBeUndefined();
	});
});

// ---------------------------------------------------------------------------
// Role enforcement
// ---------------------------------------------------------------------------

describe("Role enforcement", () => {
	let bus: TestBus | InMemoryEventBus;

	beforeEach(() => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
	});

	afterEach(async () => {
		await bus?.stop();
	});

	it("publish before start() throws", async () => {
		bus = new TestBus(resolveCoreConfig({ role: "both" }));
		await expect(
			bus.publish(createEvent(testEventDef, { id: 1 })),
		).rejects.toThrow("Bus not started");
	});

	it("publish when role=consumer throws", async () => {
		bus = new TestBus(resolveCoreConfig({ role: "consumer" }));
		await bus.start();
		await expect(
			bus.publish(createEvent(testEventDef, { id: 1 })),
		).rejects.toThrow("consumer");
	});

	it("subscribe when role=publisher auto-registers as a remote handler, not local", () => {
		bus = new TestBus(resolveCoreConfig({ role: "publisher" }));
		bus.subscribe(mockHandler("H1"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toBeUndefined();
		expect(bus.getRemoteHandlers("test.event@v1")).toHaveLength(1);
		expect(bus.getRemoteHandlers("test.event@v1")![0]!.handlerName).toBe("H1");
	});

	it("subscribe when role=both registers locally without duplicating publish targets", () => {
		bus = new TestBus(resolveCoreConfig({ role: "both" }));
		bus.subscribe(mockHandler("H1"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toHaveLength(1);
		expect(bus.getRemoteHandlers("test.event@v1")).toHaveLength(1);
		expect(bus.getPublishTargets("test.event@v1")).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// InMemory execution
// ---------------------------------------------------------------------------

describe("InMemory execution", () => {
	let bus: InMemoryEventBus;

	beforeEach(async () => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
		bus = new InMemoryEventBus(resolveCoreConfig({ role: "both" }));
		await bus.start();
	});

	afterEach(async () => {
		await bus.stop();
	});

	it("executes subscribed handler on publish and returns job IDs", async () => {
		const handler = mockHandler("H1");
		bus.subscribe(handler);

		const event = createEvent(testEventDef, { id: 123 });
		const ids = await bus.publish(event);

		expect(ids).toBeInstanceOf(Array);
		expect(ids).toContain(event.id); // InMemory should return event.id
		expect(handler.handle).toHaveBeenCalledWith(
			expect.objectContaining({
				id: event.id,
				name: "test.event",
				payload: { id: 123 },
			}),
		);
	});

	it("failing handler is isolated — other handlers still run", async () => {
		const failing: EventHandler = {
			eventName: "test.event",
			eventVersion: "v1",
			handlerName: "FailingHandler",
			handle: vi.fn().mockRejectedValue(new Error("boom")),
		};
		const succeeding = mockHandler("SucceedingHandler");

		bus.subscribe(failing);
		bus.subscribe(succeeding);

		await expect(
			bus.publish(createEvent(testEventDef, { id: 1 })),
		).resolves.toHaveLength(1);

		expect(succeeding.handle).toHaveBeenCalled();
	});
});

// ---------------------------------------------------------------------------
// Publish target resolution (LSP: every transport must resolve the same way)
// ---------------------------------------------------------------------------

describe("_resolvePublishTargets", () => {
	beforeEach(() => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
	});

	it("includes local handlers", () => {
		const bus = new TestBus(resolveCoreConfig());
		bus.subscribe(mockHandler("H1"));
		expect(bus.getPublishTargets("test.event@v1").map((h) => h.handlerName)).toEqual([
			"H1",
		]);
	});

	it("merges in remote handler stubs not covered locally", () => {
		const bus = new TestBus(resolveCoreConfig());
		bus.subscribe(mockHandler("Local"));
		bus.registerRemoteHandler({
			eventName: "test.event",
			eventVersion: "v1",
			handlerName: "Remote",
		});
		const names = bus
			.getPublishTargets("test.event@v1")
			.map((h) => h.handlerName);
		expect(names).toEqual(["Local", "Remote"]);
	});

	it("dedupes remote stubs that duplicate a local handler (local wins)", () => {
		const bus = new TestBus(resolveCoreConfig());
		bus.subscribe(mockHandler("Shared"));
		bus.registerRemoteHandler({
			eventName: "test.event",
			eventVersion: "v1",
			handlerName: "Shared",
		});
		expect(bus.getPublishTargets("test.event@v1")).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// Retry policy resolution (global default + per-handler override)
// ---------------------------------------------------------------------------

describe("_resolveRetryPolicy", () => {
	it("falls back to bus-level config.async when handler has no retry override", () => {
		const bus = new TestBus(resolveCoreConfig());
		const policy = bus.getRetryPolicy(mockHandler("H1"));
		expect(policy).toEqual({ maxRetries: 3, retryDelay: 5000 });
	});

	it("merges a per-handler override on top of the bus-level default", () => {
		const bus = new TestBus(resolveCoreConfig());
		const handler = { ...mockHandler("H1"), retry: { maxRetries: 10 } };
		const policy = bus.getRetryPolicy(handler);
		expect(policy).toEqual({ maxRetries: 10, retryDelay: 5000 });
	});
});

// ---------------------------------------------------------------------------
// Concurrency resolution (global default + per-handler override)
// ---------------------------------------------------------------------------

describe("_resolveConcurrency", () => {
	it("falls back to bus-level config.concurrency when handler has no override", () => {
		const bus = new TestBus(resolveCoreConfig());
		expect(bus.getConcurrency(mockHandler("H1"))).toBe(1);
	});

	it("uses the per-handler override when present", () => {
		const bus = new TestBus(resolveCoreConfig());
		const handler = { ...mockHandler("H1"), concurrency: 5 };
		expect(bus.getConcurrency(handler)).toBe(5);
	});
});

// ---------------------------------------------------------------------------
// Inbox guard (_guardedHandle)
// ---------------------------------------------------------------------------

describe("_guardedHandle", () => {
	beforeEach(() => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
	});

	const fakeInboxStore = (claimResult = true): InboxStore => ({
		tryClaim: vi.fn().mockResolvedValue(claimResult),
		markProcessed: vi.fn(),
		markFailed: vi.fn(),
	});

	it("calls handler directly when no inboxStore is configured", async () => {
		const bus = new TestBus(resolveCoreConfig());
		const handler = mockHandler("H1");
		const event = createEvent(testEventDef, { id: 1 });
		await bus.guardedHandle(event, handler);
		expect(handler.handle).toHaveBeenCalledWith(event);
	});

	it("skips the handler when tryClaim returns false", async () => {
		const inboxStore = fakeInboxStore(false);
		const bus = new TestBus(resolveCoreConfig({ inboxStore }));
		const handler = mockHandler("H1");
		const event = createEvent(testEventDef, { id: 1 });
		await bus.guardedHandle(event, handler);
		expect(handler.handle).not.toHaveBeenCalled();
	});

	it("marks processed after a successful claimed handle", async () => {
		const inboxStore = fakeInboxStore(true);
		const bus = new TestBus(resolveCoreConfig({ inboxStore }));
		const handler = mockHandler("H1");
		const event = createEvent(testEventDef, { id: 1 });
		await bus.guardedHandle(event, handler);
		expect(handler.handle).toHaveBeenCalledWith(event);
		expect(inboxStore.markProcessed).toHaveBeenCalledWith(event.id, "H1");
	});

	it("marks failed and rethrows when the handler throws", async () => {
		const inboxStore = fakeInboxStore(true);
		const bus = new TestBus(resolveCoreConfig({ inboxStore }));
		const boom = new Error("boom");
		const handler: EventHandler = {
			eventName: "test.event",
			eventVersion: "v1",
			handlerName: "Failing",
			handle: vi.fn().mockRejectedValue(boom),
		};
		const event = createEvent(testEventDef, { id: 1 });
		await expect(bus.guardedHandle(event, handler)).rejects.toThrow("boom");
		expect(inboxStore.markFailed).toHaveBeenCalledWith(event.id, "Failing", boom);
	});
});

// ---------------------------------------------------------------------------
// OutboxRelay
// ---------------------------------------------------------------------------

describe("OutboxRelay", () => {
	const fakePublisher = (publish = vi.fn().mockResolvedValue(["id"])): EventPublisher => ({
		start: vi.fn(),
		stop: vi.fn(),
		publish,
		registerRemoteHandler: vi.fn(),
	});

	it("claims, publishes, and marks sent", async () => {
		const event = createEvent(testEventDef, { id: 1 });
		const store: OutboxStore = {
			enqueue: vi.fn(),
			claimBatch: vi.fn().mockResolvedValueOnce([event]).mockResolvedValue([]),
			markSent: vi.fn(),
			markFailed: vi.fn(),
		};
		const publisher = fakePublisher();
		const relay = new OutboxRelay(store, publisher, { pollingIntervalMs: 10 });

		await relay.start();
		await new Promise((r) => setTimeout(r, 40));
		await relay.stop();

		expect(publisher.publish).toHaveBeenCalledWith(event);
		expect(store.markSent).toHaveBeenCalledWith([event.id]);
	});

	it("marks failed and keeps polling when publish throws", async () => {
		const event = createEvent(testEventDef, { id: 1 });
		const store: OutboxStore = {
			enqueue: vi.fn(),
			claimBatch: vi.fn().mockResolvedValueOnce([event]).mockResolvedValue([]),
			markSent: vi.fn(),
			markFailed: vi.fn(),
		};
		const publisher = fakePublisher(vi.fn().mockRejectedValue(new Error("down")));
		const relay = new OutboxRelay(store, publisher, { pollingIntervalMs: 10 });

		await relay.start();
		await new Promise((r) => setTimeout(r, 40));
		await relay.stop();

		expect(store.markFailed).toHaveBeenCalledWith(event.id, expect.any(Error));
		expect(store.markSent).not.toHaveBeenCalled();
	});
});

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

describe("Factory", () => {
	it("unregistered type throws with hint", () => {
		expect(() => createEventBus({ type: "nonexistent-transport" })).toThrow(
			/not registered/,
		);
	});

	it("type=memory returns a working bus", () => {
		const bus = createEventBus({ type: "memory" });
		expect(bus).toBeDefined();
	});

	it("resolves a registered type regardless of casing", () => {
		const bus = createEventBus({ type: "Memory" });
		expect(bus).toBeDefined();
	});

	it("does not clobber async defaults when overriding only one field", () => {
		const bus: any = createEventBus({
			type: "memory",
			async: { maxRetries: 99 } as any,
		});
		expect(bus.config.async.maxRetries).toBe(99);
		expect(bus.config.async.retryDelay).toBe(5000);
		expect(bus.config.async.eventTTL).toBe("24 hours");
	});

	it("passes through transport-specific extra fields (e.g. redis, connectionString)", () => {
		const bus: any = createEventBus({
			type: "memory",
			redis: { host: "localhost", port: 6379 },
			connectionString: "postgres://localhost/db",
		} as any);
		expect(bus.config.redis).toEqual({ host: "localhost", port: 6379 });
		expect(bus.config.connectionString).toBe("postgres://localhost/db");
	});

	it("hasEventBus reflects the registry", () => {
		expect(hasEventBus("memory")).toBe(true);
		expect(hasEventBus("MEMORY")).toBe(true);
		expect(hasEventBus("nonexistent-transport")).toBe(false);
	});

	it("getRegisteredTypes includes memory", () => {
		expect(getRegisteredTypes()).toContain("memory");
	});
});

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

describe("Config", () => {
	it("parses comma-separated EVENT_BUS_EVENTS into array", () => {
		vi.stubEnv("EVENT_BUS_EVENTS", "a, b , c");
		const config = resolveCoreConfig();
		expect(config.events).toEqual(["a", "b", "c"]);
	});

	it("async override is not clobbered", () => {
		const config = resolveCoreConfig({
			async: {
				maxRetries: 99,
				retryDelay: 1000,
				eventTTL: "1 hour",
				archiveInterval: "30 min",
				deleteArchivedInterval: "1 day",
			},
		});
		expect(config.async.maxRetries).toBe(99);
	});

	it("top-level override takes precedence over env", () => {
		vi.stubEnv("EVENT_BUS_ROLE", "consumer");
		const config = resolveCoreConfig({ role: "publisher" });
		expect(config.role).toBe("publisher");
	});
});
