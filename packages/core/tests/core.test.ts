import { describe, it, expect, vi, beforeEach } from "vitest";
import {
	CoreEventBus,
	resolveCoreConfig,
	EventHandler,
	InMemoryEventBus,
	createEvent,
	defineEvent,
} from "../src";

class TestBus extends CoreEventBus {
	async start() {
		this.started = true;
	}
	async stop() {
		this.started = false;
	}
	protected async _publishInternal() {}
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

describe("Event Bus Core - Filtering", () => {
	beforeEach(() => {
		vi.stubEnv("EVENT_BUS_EVENTS", "*");
		vi.stubEnv("EVENT_BUS_WORKERS", "*");
	});

	it("should filter handlers by name using EVENT_BUS_WORKERS", () => {
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

	it("should parse comma-separated env strings into arrays", () => {
		vi.stubEnv("EVENT_BUS_EVENTS", "a, b , c");
		const config = resolveCoreConfig();
		expect(config.events).toEqual(["a", "b", "c"]);
	});

	it("should execute subscribed handlers when event is published", async () => {
		const bus = new InMemoryEventBus(resolveCoreConfig({ role: "both" }));
		await bus.start();

		const handler = mockHandler("H1");
		bus.subscribe(handler);

		const eventDef = defineEvent<{ id: number }>("test.event", "v1");
		const event = createEvent(eventDef, { id: 123 });

		await bus.publish(event);

		expect(handler.handle).toHaveBeenCalledWith(
			expect.objectContaining({
				name: "test.event",
				payload: { id: 123 },
			}),
		);
	});

	it("should block everything when EVENT_BUS_WORKERS is specific and no handler matches", () => {
		vi.stubEnv("EVENT_BUS_WORKERS", "NoneMatch");
		const bus = new TestBus(resolveCoreConfig());

		bus.subscribe(mockHandler("SomeHandler"));
		expect(bus.getRegisteredHandlers("test.event@v1")).toBeUndefined();
	});
});
