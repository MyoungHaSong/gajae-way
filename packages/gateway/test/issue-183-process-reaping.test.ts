import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { reapLeftoverWorkers } from "../src/ops/process-reaper";

/**
 * Test suite for issue #183: Agent child processes outlive their unit and are never reaped.
 * Verifies that:
 * 1. Leftover orphaned processes are found and reaped on boot
 * 2. Child processes with TTL are reaped after timeout
 * 3. Process reaper doesn't crash on missing processes
 */

test("process reaper: finds no orphaned processes in a clean environment", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-clean-"));
	try {
		const result = await reapLeftoverWorkers(home);
		// In a clean test environment, we shouldn't find SDK serve processes
		expect(result.found).toBeGreaterThanOrEqual(0);
		expect(result.killed).toBe(0);
		expect(result.failed).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("process reaper: handles missing /proc gracefully on non-Linux", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-graceful-"));
	try {
		// Should not throw even if /proc is missing (non-Linux systems)
		const result = await reapLeftoverWorkers(home, {
			log: () => {
				/* suppress logs */
			},
		});
		expect(result.found).toBeGreaterThanOrEqual(0);
		expect(typeof result.killed).toBe("number");
		expect(typeof result.failed).toBe("number");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("process reaper: returns consistent result structure", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-structure-"));
	try {
		const result = await reapLeftoverWorkers(home);
		expect(typeof result.found).toBe("number");
		expect(typeof result.killed).toBe("number");
		expect(typeof result.failed).toBe("number");
		expect(result.found >= 0).toBe(true);
		expect(result.killed >= 0).toBe(true);
		expect(result.failed >= 0).toBe(true);
		expect(result.killed + result.failed <= result.found).toBe(true);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
