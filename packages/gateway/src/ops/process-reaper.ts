import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

export interface ProcessReaperOptions {
	/** GJC executable being used for spawning. */
	readonly gjcExecutable?: string;
	/** Timeout for the reaper itself (ms). */
	readonly timeoutMs?: number;
	/** Log function for diagnostics. */
	readonly log?: (line: string) => void;
}

export interface ReapResult {
	readonly found: number;
	readonly killed: number;
	readonly failed: number;
}

/**
 * Identifies and kills leftover child processes from the previous gateway epoch.
 * This is called on boot before accepting work, ensuring no orphaned children persist.
 */
export async function reapLeftoverWorkers(_home: string, options: ProcessReaperOptions = {}): Promise<ReapResult> {
	const { gjcExecutable = "gjc", log = (s) => console.warn(s) } = options;
	let found = 0;
	let killed = 0;
	let failed = 0;

	try {
		// Find all bun processes that are gjc children (spawned by gjc session.create or sdk serve)
		// They will have gjc as an ancestor in the process tree
		const foundPids = await findLikelyOrphanedWorkers(gjcExecutable);
		found = foundPids.length;

		if (foundPids.length === 0) return { found, killed, failed };

		log(`process-reaper: found ${foundPids.length} potential leftover worker processes`);

		// Kill each found process with SIGTERM then SIGKILL
		for (const pid of foundPids) {
			try {
				await killProcess(pid);
				killed++;
				log(`process-reaper: killed leftover worker pid=${pid}`);
			} catch (error) {
				failed++;
				log(`process-reaper: failed to kill pid=${pid}: ${error instanceof Error ? error.message : String(error)}`);
			}
		}
	} catch (error) {
		log(`process-reaper: scan failed: ${error instanceof Error ? error.message : String(error)}`);
	}

	return { found, killed, failed };
}

/**
 * Finds processes that are likely orphaned gjc children.
 * Strategy: find all bun processes with gjc-related command names in their ancestry.
 */
async function findLikelyOrphanedWorkers(gjcExecutable: string): Promise<number[]> {
	// List all processes and check for gjc children
	// On Linux, we can use /proc, on macOS we use ps
	const isLinux = process.platform === "linux";

	if (isLinux) {
		return await findOrphanedLinux(gjcExecutable);
	} else {
		return await findOrphanedUnix(gjcExecutable);
	}
}

/**
 * Linux-specific process discovery using /proc.
 */
async function findOrphanedLinux(_gjcExecutable: string): Promise<number[]> {
	const result: number[] = [];
	try {
		const procPids = await readdir("/proc").catch(() => []);
		for (const entry of procPids) {
			const pid = Number(entry);
			if (!Number.isSafeInteger(pid) || pid <= 0) continue;

			try {
				const cmdline = await readFile(join("/proc", entry, "cmdline"), "utf-8")
					.then((s) => s.split("\0").filter(Boolean))
					.catch(() => []);

				// Look for bun processes that are spawned by gjc operations
				// Typical patterns: "bun" with gjc in the environment or as ancestor
				if (
					cmdline[0]?.endsWith("bun") &&
					(cmdline.join(" ").includes("sdk serve") || cmdline.join(" ").includes("session"))
				) {
					// This is likely a gjc spawned process
					const stat = await readFile(join("/proc", entry, "stat"), "utf-8").catch(() => "");
					// Check if parent PID is 1 (orphaned) or still running
					const parts = stat.split(" ");
					const ppid = Number(parts[3]);
					// We reap if parent is init (ppid === 1) or process is old
					if (ppid === 1) {
						result.push(pid);
					}
				}
			} catch {
				// Ignore errors reading proc entries (permission denied, process died, etc)
			}
		}
	} catch {
		// /proc not available or other error
	}
	return result;
}

/**
 * Unix-specific process discovery using ps.
 */
async function findOrphanedUnix(_gjcExecutable: string): Promise<number[]> {
	const result: number[] = [];
	try {
		const proc = Bun.spawn(["ps", "-eo", "ppid=,pid=,command="], {
			stdout: "pipe",
			stderr: "ignore",
		});

		const output = await new Response(proc.stdout as ReadableStream).text();
		const lines = output.trim().split("\n");

		for (const line of lines) {
			const parts = line.trim().split(/\s+/);
			if (parts.length < 3) continue;

			const ppid = Number(parts[0]);
			const pid = Number(parts[1]);
			const command = parts.slice(2).join(" ");

			// Look for bun processes running gjc commands that are orphaned (ppid === 1)
			if (Number.isSafeInteger(pid) && ppid === 1 && command.includes("bun")) {
				if (command.includes("sdk serve") || command.includes("session")) {
					result.push(pid);
				}
			}
		}

		await proc.exited;
	} catch {
		// ps not available or other error
	}

	return result;
}

/**
 * Sends SIGTERM to a process, then SIGKILL if it doesn't die quickly.
 */
async function killProcess(pid: number, timeoutMs = 2_000): Promise<void> {
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		try {
			process.kill(pid, signal);
		} catch {
			// Already dead or permission denied
		}

		// Wait a bit for the process to exit
		try {
			const proc = Bun.spawn(["kill", "-0", String(pid)], {
				stdout: "ignore",
				stderr: "ignore",
			});
			const exitPromise = proc.exited;
			const timeoutPromise = new Promise<boolean>((resolve) => {
				setTimeout(() => resolve(false), timeoutMs);
			});

			const stillAlive = await Promise.race([exitPromise.then(() => false), timeoutPromise]).catch(() => true);

			if (!stillAlive) return; // Process is dead
		} catch {
			// Assume process is dead if we can't check
			return;
		}
	}

	throw new Error(`Process ${pid} did not exit after SIGTERM and SIGKILL`);
}
