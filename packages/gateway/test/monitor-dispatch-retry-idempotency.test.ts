import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CATCH_ALL_EVENT_ORIGIN, monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionRequestResult } from "../src/orchestrator/session-port";
import { Store } from "../src/store";
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS } from "../src/store/groups/monitors";
import { sessionPortFromScript } from "./session-port.fake";

const SESSION_ID = "test-session-187";
const OP_REF = "gw-m-issue-187";

let home = "";
let database: Store | undefined;
let propagator: MonitorPropagator | undefined;

afterEach(async () => {
	await propagator?.drain();
	propagator = undefined;
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function fixture(
	options: {
		eventType?: string;
		declaredEventTypes?: string[];
		eventCount?: number;
		persistBinding?: boolean;
		contextFailureRollThreshold?: number;
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retry-idempotency-"));
	const store = await Store.open(join(home, "gateway.db"));
	database = store;
	const registry = new MonitorRegistry(store);
	const eventType = options.eventType ?? "test.retry";
	const declaredEventTypes = options.declaredEventTypes ?? [eventType];
	const monitor = registry.add({
		name: "retry-test",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: declaredEventTypes,
		burstPolicy: "serialize",
	});
	const sessionOrigin = monitorSessionOrigin(
		monitor.monitorId,
		monitor.eventTypes.includes(eventType) ? eventType : CATCH_ALL_EVENT_ORIGIN.conversationId,
	);
	const sessionOriginKey = originKey(sessionOrigin);
	const eventIds = Array.from({ length: options.eventCount ?? 1 }, () => crypto.randomUUID());
	const eventId = eventIds[0];
	if (!eventId) throw new Error("fixture requires at least one monitor event");
	const pointerKey = `monitor_authoring_turn:${eventId}`;
	const pointer = JSON.stringify({ sessionId: SESSION_ID, opRef: OP_REF, eventIds });
	let bindSessionId = SESSION_ID;
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: bindSessionId }),
		respond: async () => "[]",
	});
	store.transaction((tx) => {
		if (options.persistBinding !== false) store.bindings.bindAtEpoch(tx, sessionOriginKey, SESSION_ID, 0);
		for (const currentId of eventIds) {
			store.monitors.eventCreate(tx, {
				eventId: currentId,
				monitorId: monitor.monitorId,
				eventType,
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			store.monitors.eventUpdate(tx, currentId, "failed");
			for (let attempt = 0; attempt < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS; attempt++)
				store.monitors.eventIncrementAttempts(tx, currentId);
			store.core.metaSet(`monitor_authoring_turn:${currentId}`, pointer);
		}
	});
	const pipeline = new MonitorPropagator({
		database: store,
		registry,
		sessionPort,
		memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(store),
		emit: () => {},
		repo: home,
		...(options.contextFailureRollThreshold === undefined
			? {}
			: { contextFailureRollThreshold: options.contextFailureRollThreshold }),
	});
	propagator = pipeline;
	return {
		eventId,
		eventIds,
		monitor,
		pipeline,
		pointer,
		pointerKey,
		sessionOriginKey,
		sessionPort,
		setBindSessionId: (sessionId: string) => {
			bindSessionId = sessionId;
		},
		store,
	};
}

function countStatusCalls(port: ReturnType<typeof sessionPortFromScript>): () => number {
	let count = 0;
	const status = port.status.bind(port);
	port.status = async (input) => {
		count++;
		return status(input);
	};
	return () => count;
}

test("issue #187: an exhausted event stays recoverable while its catch-all authoring turn is in flight", async () => {
	const state = await fixture({ eventType: "test.undeclared", declaredEventTypes: ["test.declared"] });
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "in_flight");
	const statusCalls = countStatusCalls(state.sessionPort);

	await state.pipeline.reconcile();

	expect(statusCalls()).toBe(1);
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
	expect(state.sessionPort.sends).toHaveLength(0);
});

test("issue #187: an unknown turn status preserves the event and authoring-turn pointer", async () => {
	const state = await fixture();
	const statusCalls = countStatusCalls(state.sessionPort);

	await state.pipeline.reconcile();

	expect(statusCalls()).toBe(1);
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
});

test("issue #187: an unreadable turn status is not treated as proof the turn stopped", async () => {
	const state = await fixture();
	state.sessionPort.status = async () => {
		throw new Error("status transport unavailable");
	};

	await state.pipeline.reconcile();

	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
});

test("issue #187: a completed exhausted turn is resumed and its authored result is retained", async () => {
	const state = await fixture({
		eventType: "test.undeclared",
		declaredEventTypes: ["test.declared"],
		eventCount: 2,
		contextFailureRollThreshold: 1,
	});
	const originalRequest = state.sessionPort.request.bind(state.sessionPort);
	state.sessionPort.request = async (input) =>
		({
			receipt: { sessionId: input.sessionId, operationRef: input.opRef } as never,
			status: {
				operationRef: input.opRef,
				status: { status: "terminal_ok" },
				summaryCompleted: true,
			},
			assistant: { text: "", pages: 1, complete: true },
		}) satisfies SessionRequestResult;
	const contextFailureId = await state.pipeline.submitAwaitable(state.monitor.monitorId, "test.undeclared", {});
	state.sessionPort.request = originalRequest;
	expect(state.pipeline.sessionSafetyState(state.sessionOriginKey).pendingRoll).toBe(
		"context_failures_native_compaction_unavailable",
	);
	state.store.transaction((tx) => state.store.monitors.eventUpdate(tx, contextFailureId, "failed_no_retry"));
	const rotatedSessionId = "test-session-187-rotated";
	const rotatedEpoch = state.store.transaction((tx) => state.store.bindings.rotateEpoch(tx, state.sessionOriginKey));
	state.store.transaction((tx) =>
		state.store.bindings.bindAtEpoch(tx, state.sessionOriginKey, rotatedSessionId, rotatedEpoch),
	);
	state.setBindSessionId(rotatedSessionId);
	const bindCountBeforeRecovery = state.sessionPort.binds.length;
	const output = JSON.stringify(
		state.eventIds.map((eventId, index) => ({ eventId, note: `completed result ${index + 1}` })),
	);
	state.sessionPort.seedAcceptedSend(
		{ sessionId: SESSION_ID, repo: home, text: "previous authoring prompt", opRef: OP_REF },
		"terminal_ok",
		output,
	);

	await state.pipeline.reconcile();

	expect(state.eventIds.map((eventId) => state.store.monitors.authoredOutput(eventId))).toEqual([
		"completed result 1",
		"completed result 2",
	]);
	expect(state.eventIds.map((eventId) => state.store.monitors.eventGet(eventId)?.stage)).toEqual([
		"authored_no_delivery",
		"authored_no_delivery",
	]);
	expect(state.sessionPort.sends.map((send) => send.opRef)).toEqual([OP_REF]);
	expect(state.sessionPort.sendAttempts.map((send) => send.opRef)).toEqual([OP_REF, OP_REF]);
	expect(state.sessionPort.binds).toHaveLength(bindCountBeforeRecovery);
	expect(state.pipeline.sessionSafetyState(state.sessionOriginKey).pendingRoll).toBe(
		"context_failures_native_compaction_unavailable",
	);
	expect(state.store.bindings.getSessionRecord(state.sessionOriginKey)).toEqual({
		sessionId: rotatedSessionId,
		epoch: rotatedEpoch,
	});
	expect(state.eventIds.map((eventId) => state.store.core.metaGet(`monitor_authoring_turn:${eventId}`))).toEqual([
		undefined,
		undefined,
	]);
});

test("an exhausted event is terminalized when its recorded authoring turn failed", async () => {
	const state = await fixture({ persistBinding: false });
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");

	await state.pipeline.reconcile();

	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed_no_retry");
	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
});
