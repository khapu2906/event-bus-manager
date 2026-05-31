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

	it("subscribe when role=publisher warns and skips", () => {
		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		bus = new TestBus(resolveCoreConfig({ role: "publisher" }));
		bus.subscribe(mockHandler("H1"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toBeUndefined();
		warnSpy.mockRestore();
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
