import { existsSync, lstatSync, realpathSync, rmSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import type { LogLevel } from "@gajae-gateway/log";
import { type LaneCapacityDetail, ProtocolError } from "@gajae-gateway/protocol";
import { type LaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { DEFAULT_WORK_IDLE_RETIRE_MS, DEFAULT_WORK_MAX_LANES } from "../config";
import type { Store } from "../store";
import { runGit } from "./git-command";
import { sanitizeDiagnostic } from "./rebind";
import type { SessionLiveness, SessionPort } from "./session-port";

export const WORK_LANE_PREFIX = "work/task/";

/** Lane-job states after which a bound session is dead weight and may be retired by the sweep. */
const TERMINAL_JOB_STATES = new Set(["done", "aborted"]);
/**
 * Attempt end states the gateway recorded WITHOUT broker proof that the
 * operation stopped: a reaped wait (`attempt_ended`), a crash-left attempt
 * (`terminal_uncertain`), and a local `failed` (any exception on the request
 * path, including a status/transport error after an accepted send). Retiring
 * such a lane needs runtime evidence; only `completed` is broker-proven.
 */
const UNPROVEN_END_STATES = new Set(["attempt_ended", "terminal_uncertain", "terminal_missing_receipt", "failed"]);
/**
 * How long a dead/disowned verdict must persist, across at least two probes,
 * before unattended reconciliation acts on it. An overloaded broker answers
 * inspect with `live: false` or `endpoint_stale` for hosts that are still
 * working, and one such answer used to end the lane on the spot.
 */
export const DEFAULT_DEAD_LANE_CONFIRM_MS = 3 * 60_000;
/** Host or gateway-observed session activity this recent contradicts a dead verdict. */
export const DEAD_LANE_RECENT_ACTIVITY_MS = 2 * 60_000;

/**
 * One liveness probe of a lane host read as evidence. `dead` (not live, or
 * disowned such as `endpoint_stale`) is only a suspicion until it repeats (see
 * {@link DeadVerdictStreaks}); activity the inspect reply or the gateway itself
 * saw within DEAD_LANE_RECENT_ACTIVITY_MS contradicts it. An unanswered probe
 * (timeout, broker unavailable, no `live` field) is `indeterminate`, never dead.
 */
export function laneHostVerdict(
	liveness: SessionLiveness,
	now: number,
	lastActivityAt?: string | null,
): "live" | "dead" | "indeterminate" {
	if (liveness.live !== false && !liveness.disowned) return liveness.live === true ? "live" : "indeterminate";
	const observed = lastActivityAt ? Date.parse(lastActivityAt) : Number.NaN;
	const latest = Math.max(
		liveness.activity?.at ?? Number.NEGATIVE_INFINITY,
		Number.isFinite(observed) ? observed : Number.NEGATIVE_INFINITY,
	);
	return now - latest < DEAD_LANE_RECENT_ACTIVITY_MS ? "live" : "dead";
}

/**
 * Repeated-death bookkeeping shared by the dead-lane sweep and work recovery.
 * A key is confirmed dead only after `dead` verdicts on at least two probes
 * spanning `confirmMs`. A `live` verdict clears the streak; an `indeterminate`
 * one leaves it untouched, so silence neither confirms nor forgives.
 */
export class DeadVerdictStreaks {
	readonly #streaks = new Map<string, { readonly since: number; readonly count: number }>();

	constructor(readonly confirmMs: number) {
		if (!Number.isSafeInteger(confirmMs) || confirmMs < 0)
			throw new Error(`deadConfirmMs must be a non-negative integer, got ${confirmMs}`);
	}

	/** Records one verdict; `first` marks the probe that opened a streak. */
	record(
		key: string,
		verdict: "live" | "dead" | "indeterminate",
		now: number,
	): { readonly confirmed: boolean; readonly first: boolean } {
		if (verdict === "live") this.#streaks.delete(key);
		if (verdict !== "dead") return { confirmed: false, first: false };
		const prior = this.#streaks.get(key);
		const streak = { since: prior?.since ?? now, count: (prior?.count ?? 0) + 1 };
		this.#streaks.set(key, streak);
		return { confirmed: streak.count >= 2 && now - streak.since >= this.confirmMs, first: !prior };
	}

	delete(key: string): void {
		this.#streaks.delete(key);
	}

	/** Forgets every key not in `keys` (lanes rebound or retired meanwhile). */
	retain(keys: ReadonlySet<string>): void {
		for (const key of this.#streaks.keys()) if (!keys.has(key)) this.#streaks.delete(key);
	}
}

/**
 * Durable identity of one work lane job. The jobId is INJECTIVE: each
 * UTF-8 byte of the exact name becomes two hex digits, so distinct names
 * (Foo/foo, a.b/a_b) always map to distinct ids while staying within the
 * [a-z0-9-] jobId alphabet. 64-byte names cap at 128 hex chars + the prefix,
 * inside the schema's id length bound.
 */
export function laneJobIdentity(workName: string): { jobId: string; laneKey: string } {
	return { jobId: `lanejob-${Buffer.from(workName, "utf8").toString("hex")}`, laneKey: `work-${workName}` };
}

export function managedLaneBranchName(workName: string): string {
	return `work/${Buffer.from(workName, "utf8").toString("hex")}`;
}

export function managedLaneRepositoryPath(laneRoot: string, workName: string): string {
	return join(resolve(laneRoot), "repositories", Buffer.from(workName, "utf8").toString("hex"));
}

export function managedLaneWorktreePath(laneRoot: string, workName: string): string {
	return join(resolve(laneRoot), "lanes", Buffer.from(workName, "utf8").toString("hex"));
}

export function workSessionKey(name: string): string {
	return `${WORK_LANE_PREFIX}${name}`;
}

export interface ActiveLane {
	readonly name: string;
	readonly sessionKey: string;
	readonly sessionId: string;
	readonly lastActivityAt: string | null;
	readonly idleMs: number;
	/** Lane-job state; `unknown` when no job row exists, `corrupt` when the row cannot be parsed. */
	readonly state: string;
	/**
	 * True while the lane must not be retired: the durable job has an attempt
	 * without a recorded end, or the job record is corrupt and cannot prove
	 * there is none. Missing evidence is never "settled".
	 */
	readonly attemptOpen: boolean;
}

/**
 * Admission may retire only a settled lane that no longer owns live follow-up:
 * an `awaiting_checks` lane keeps its session for the checks poller, and
 * retiring it would conclude that cycle just to free a slot.
 */
function admissionRetirable(lane: ActiveLane): boolean {
	return !lane.attemptOpen && lane.state !== "awaiting_checks";
}

export type LaneRetireOutcome =
	| {
			readonly retired: true;
			readonly sessionKey: string;
			readonly sessionId: string;
			readonly closed: boolean;
			readonly forced?: boolean;
	  }
	| { readonly retired: false; readonly sessionKey: string; readonly reason: string };

export type SweepReason = "idle" | "job_done" | "job_aborted";

/**
 * What a sweep or admission observed when it nominated a lane; re-proven under
 * the lane lock. `admission` nominates any settled lane to free a slot for a
 * new lane name, so its only predicate is that no attempt is open.
 */
export interface SweepNomination {
	readonly sessionId: string;
	readonly reason: SweepReason | "admission";
	/** The sweep's clock; the locked recheck evaluates fresh state at this time or later. */
	readonly now: number;
}

export type LaneForceRetireReason = "session_dead" | "session_disowned" | "operator_retired";
type ForceRetireSettlement = (name: string, opRef: string, reason: LaneForceRetireReason, endedAt: string) => boolean;

export interface LaneGovernorOptions {
	readonly database: Store;
	readonly sessionPort: SessionPort;
	/** Configured lane root used to prove managed repository ownership before deletion. */
	readonly laneRoot?: string;
	readonly maxLanes?: number;
	readonly idleRetireMs?: number;
	/** See DEFAULT_DEAD_LANE_CONFIRM_MS; 0 still requires two sweeps. */
	readonly deadConfirmMs?: number;
	readonly now?: () => number;
	readonly log?: (line: string, level?: LogLevel) => void;
}

/**
 * Gateway-owned admission and retirement for asynchronous work lanes.
 *
 * Every lane is a bound `work/task/<name>` origin hosted by the private broker,
 * so the gateway can count, cap, and close them. Retirement is `session.close`
 * plus an epoch bump: the saved session stays in the broker index (deleting it
 * would trip gjc's global cleanup fence) while the next start/run for that
 * name creates a fresh session instead of resuming a stale one.
 */
export class LaneGovernor {
	readonly #database: Store;
	readonly #port: SessionPort;
	readonly #laneRoot: string | undefined;
	readonly maxLanes: number;
	readonly idleRetireMs: number;
	readonly #now: () => number;
	/** Dead verdicts per session id, across retireAllDead sweeps. */
	readonly #deadStreaks: DeadVerdictStreaks;
	readonly #log: (line: string, level?: LogLevel) => void;
	#recoveryGate?: () => Promise<void>;
	#forceRetireSettlement?: ForceRetireSettlement;
	#stopped = false;
	readonly #mutations = new Set<Promise<LaneRetireOutcome>>();

	constructor(options: LaneGovernorOptions) {
		this.#database = options.database;
		this.#port = options.sessionPort;
		this.#laneRoot = options.laneRoot === undefined ? undefined : resolve(options.laneRoot);
		this.maxLanes = positiveInteger(options.maxLanes, DEFAULT_WORK_MAX_LANES, "maxLanes");
		this.idleRetireMs = positiveInteger(options.idleRetireMs, DEFAULT_WORK_IDLE_RETIRE_MS, "idleRetireMs");
		this.#deadStreaks = new DeadVerdictStreaks(options.deadConfirmMs ?? DEFAULT_DEAD_LANE_CONFIRM_MS);
		this.#now = options.now ?? (() => Date.now());
		this.#log = options.log ?? ((line, level) => console[level ?? "info"](line));
	}

	/** Installed by the sole attempt owner; recovery registration precedes retirement. */
	setRecoveryGate(gate: () => Promise<void>): void {
		this.#recoveryGate = gate;
		this.#stopped = false;
	}

	/** Installed by the attempt owner so forced retirement settles history and runtime atomically. */
	setForceRetireSettlement(settle: ForceRetireSettlement): void {
		this.#forceRetireSettlement = settle;
	}

	async stop(): Promise<void> {
		this.#stopped = true;
		await Promise.allSettled([...this.#mutations]);
	}

	activeLanes(now = this.#now()): ActiveLane[] {
		return this.#database.bindings.workLaneRows().map((row) => {
			const name = row.origin_key.slice(WORK_LANE_PREFIX.length);
			const job = this.#job(name);
			const activityMs = row.last_activity_at ? Date.parse(row.last_activity_at) : Number.NaN;
			return {
				name,
				sessionKey: row.origin_key,
				sessionId: row.gjc_session_id,
				lastActivityAt: row.last_activity_at,
				idleMs: Number.isFinite(activityMs) ? Math.max(0, now - activityMs) : Number.POSITIVE_INFINITY,
				state: job === "corrupt" ? "corrupt" : (job?.state ?? "unknown"),
				attemptOpen: job === "corrupt" || (job?.attempts.some((attempt) => attempt.endedAt === undefined) ?? false),
			};
		});
	}

	/** Lanes counted against maxLanes: quarantined lanes do not count. */
	#countedLanes(now = this.#now()): ActiveLane[] {
		return this.activeLanes(now).filter(
			(lane) => !this.#database.bindings.isQuarantined("work", laneJobIdentity(lane.name).jobId),
		);
	}

	/**
	 * Frees one slot for a new lane name when the cap is reached, by retiring a
	 * settled lane (no open attempt): terminal jobs first, then the idlest. A
	 * finished lane otherwise keeps its slot until the idle sweep, so a full cap
	 * of finished lanes would refuse new work while nothing runs. The retired
	 * lane keeps its job record and worktree; its next start binds a fresh
	 * session. Runs outside the admission lock: retirement takes the victim's
	 * lane lock, and a resuming lane holds its own lane lock while it waits for
	 * admission. `assertAdmission` re-checks capacity under the admission lock.
	 */
	async makeRoom(name: string): Promise<void> {
		if (this.#stopped) return;
		const now = this.#now();
		const lanes = this.#countedLanes(now);
		if (lanes.some((lane) => lane.name === name) || lanes.length < this.maxLanes) return;
		const candidates = lanes
			.filter(admissionRetirable)
			.sort(
				(left, right) =>
					Number(TERMINAL_JOB_STATES.has(right.state)) - Number(TERMINAL_JOB_STATES.has(left.state)) ||
					right.idleMs - left.idleMs,
			);
		for (const lane of candidates) {
			try {
				const outcome = await this.retire(lane.name, "admission", {
					sessionId: lane.sessionId,
					reason: "admission",
					now,
				});
				if (outcome.retired) return;
				this.#log(
					`lane_admission_retire_skipped name=${lane.name} for=${name} reason=${sanitizeDiagnostic(outcome.reason)}`,
				);
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_retire_failed name=${lane.name} session=${lane.sessionId} reason=${detail}`);
			}
		}
	}

	/**
	 * A name that already owns a bound lane is always admitted (it resumes its
	 * own session). A new name past the cap is refused with the idlest lanes as
	 * retirement candidates; callers run `makeRoom` first, so a refusal means
	 * every counted lane has an open attempt or could not be proven settled.
	 * Quarantined lanes do not count against maxLanes.
	 */
	assertAdmission(name: string): void {
		const { jobId } = laneJobIdentity(name);
		if (this.#database.bindings.isQuarantined("work", jobId))
			throw new ProtocolError("verb_failed", "work lane belongs to a quarantined broker authority", {
				reasonCode: "broker_authority_quarantined",
				jobId,
				name,
			});
		const lanes = this.#countedLanes();
		if (lanes.some((lane) => lane.name === name)) return;
		if (lanes.length < this.maxLanes) return;
		const detail: LaneCapacityDetail = {
			maxLanes: this.maxLanes,
			active: lanes.length,
			candidates: [...lanes]
				.sort((left, right) => right.idleMs - left.idleMs)
				.map((lane) => ({
					name: lane.name,
					idleMs: Number.isFinite(lane.idleMs) ? lane.idleMs : -1,
					state: lane.state,
				})),
		};
		throw new ProtocolError(
			"lane_capacity",
			`work lanes are at capacity (${lanes.length}/${this.maxLanes}) and none could be retired automatically; wait for a lane to finish before starting ${name}`,
			detail,
		);
	}

	/**
	 * Serialized with the lane's short mutation lock, never its response wait.
	 * The binding identity observed
	 * by the caller (sweep snapshot, operator view) is re-proven under the lock:
	 * a lane that was reused or rebound meanwhile is left alone.
	 *
	 * `expected` carries the nominating snapshot: the session id it selected and
	 * the predicate it selected on. Both are re-proven under the lock; a lane
	 * reused on the same session (no longer idle, job no longer terminal) or
	 * rebound to a new one is left alone.
	 */
	retire(name: string, reason: string, expected?: SweepNomination): Promise<LaneRetireOutcome> {
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({
				retired: false,
				sessionKey: workSessionKey(name),
				reason: "broker_authority_quarantined",
			});
		if (this.#stopped)
			return Promise.resolve({ retired: false, sessionKey: workSessionKey(name), reason: "gateway is stopping" });
		const task = (async () => {
			await this.#recoveryGate?.();
			if (this.#stopped)
				return { retired: false as const, sessionKey: workSessionKey(name), reason: "gateway is stopping" };
			return this.#retire(name, reason, expected);
		})();
		this.#mutations.add(task);
		void task.then(
			() => this.#mutations.delete(task),
			() => this.#mutations.delete(task),
		);
		return task;
	}

	#retire(name: string, reason: string, expected?: SweepNomination): Promise<LaneRetireOutcome> {
		const sessionKey = workSessionKey(name);
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({ retired: false, sessionKey, reason: "broker_authority_quarantined" });
		// Refuse an unsettled lane now rather than queuing retirement behind its
		// turn; the same check repeats under the lock because a run may start
		// before we acquire it.
		const preflight = this.activeLanes().find((candidate) => candidate.name === name);
		if (preflight?.attemptOpen) return Promise.resolve(unsettled(sessionKey, preflight));
		return this.#port.runExclusive(sessionKey, async () => {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
				return { retired: false, sessionKey, reason: "broker_authority_quarantined" };
			if (this.#stopped) return { retired: false, sessionKey, reason: "gateway is stopping" };
			const lane = this.activeLanes(expected ? Math.max(expected.now, this.#now()) : undefined).find(
				(candidate) => candidate.name === name,
			);
			if (!lane) return { retired: false, sessionKey, reason: "no bound lane for that name" };
			if (expected && lane.sessionId !== expected.sessionId)
				return { retired: false, sessionKey, reason: `lane was rebound to ${lane.sessionId} since it was selected` };
			if (lane.attemptOpen) return unsettled(sessionKey, lane);
			if (
				expected &&
				(expected.reason === "admission" ? !admissionRetirable(lane) : this.#sweepReason(lane) !== expected.reason)
			)
				return { retired: false, sessionKey, reason: `lane no longer qualifies for ${expected.reason} retirement` };
			const job = this.#job(name);
			const record = job === "corrupt" ? undefined : job;
			// Try job record repo first, then owned binding repo, then process.cwd() fallback.
			// In broker mode: if there's no job record and no owned binding, the lane is an orphan.
			// In singleton mode: process.cwd() is safe (no broker enforcing repo matching).
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				// No job record and no owned binding repo.
				// Check if we're in broker mode: if so, this is an orphan.
				// If not, fall back to process.cwd() (safe in singleton mode).
				if (this.#database.bindings.isBrokerMode()) {
					// Broker mode: cannot close safely without knowing the repo.
					this.#log(`lane_retire_failed name=${name} session=${lane.sessionId} reason=orphan_no_repo_found`);
					return { retired: false, sessionKey, reason: "orphan lane: no job record and no owned binding repo" };
				}
				// Singleton mode: safe to use process.cwd().
				repo = process.cwd();
			}
			// The ledger's last attempt ended, but only the broker knows whether
			// its operation actually reached a terminal state: a timed-out wait
			// or a crash-left attempt may still be running.
			const runtime = await this.#runtimeSettled(record, lane.sessionId, repo);
			if (!runtime.settled) return { retired: false, sessionKey, reason: runtime.reason };
			let closed = false;
			try {
				await this.#port.close({ sessionId: lane.sessionId, repo });
				closed = true;
			} catch (error) {
				// An unconfirmed close must not release the slot: a worker that is
				// still alive would then be uncounted, which is the exact failure
				// this governor exists to prevent. Only broker liveness proving the
				// session gone lets the binding clear.
				const detail = sanitizeDiagnostic(diagnostic(error));
				const liveness = await this.#safeLiveness(lane.sessionId, repo);
				if (liveness.live !== false && !liveness.disowned) {
					this.#log(
						`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=retained`,
						"error",
					);
					return {
						retired: false,
						sessionKey,
						reason: `session.close failed and the session is not proven gone: ${detail}`,
					};
				}
				this.#log(
					`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=session_gone`,
					"error",
				);
			}
			// A settled attempt can still own an open checks cycle. Left open, the checks
			// poller keeps re-steering red checks into a fresh session after retirement.
			// Settle it only once the session is closed or proven gone: a retained lane
			// must keep its checks monitoring.
			const lastAttempt = record?.attempts.at(-1);
			if (record?.state === "awaiting_checks" && lastAttempt) {
				const endedAt = new Date(this.#now()).toISOString();
				if (!this.#forceRetireSettlement?.(name, lastAttempt.opRef, "operator_retired", endedAt)) {
					const detail = "awaiting_checks cycle could not be settled";
					this.#log(`lane_retire_failed name=${name} opRef=${lastAttempt.opRef} reason=${detail}`);
					return { retired: false, sessionKey, reason: detail };
				}
			}
			// Only a non-forced operator retirement (no sweep nomination) deletes the
			// runtime-created worktree and branch. Unattended sweep/force retirement keeps
			// the durable lane path and its unpushed commits resumable.
			const repositoryPath = record?.lane.repositoryPath ?? record?.lane.repository;
			if (!expected && record?.lane.worktreePath && repositoryPath) {
				try {
					await this.#cleanupWorktree(
						name,
						lane.sessionId,
						record.lane.worktreePath,
						repositoryPath,
						record.lane.repositoryPath !== undefined,
					);
				} catch (error) {
					const detail = sanitizeDiagnostic(error instanceof Error ? error.message : String(error));
					this.#log(`lane_worktree_cleanup_failed name=${name} session=${lane.sessionId} reason=${detail}`, "error");
					return { retired: false, sessionKey, reason: `worktree cleanup failed: ${detail}` };
				}
			}

			this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, sessionKey));
			this.#log(`lane_retired name=${name} session=${lane.sessionId} reason=${reason} closed=${closed}`, "info");
			return { retired: true, sessionKey, sessionId: lane.sessionId, closed };
		});
	}

	/**
	 * Retires lanes whose job is terminal or that have been quiet past the idle
	 * ceiling. The snapshot only nominates; `retire` re-proves the binding
	 * identity and eligibility under the lane lock before closing anything.
	 * Per-lane errors are caught and logged; one lane's failure does not abort the sweep.
	 * Dead/disowned lane reconciliation is handled separately via retireAllDead().
	 */
	async sweep(now = this.#now()): Promise<number> {
		if (this.#stopped) return 0;
		let retired = 0;
		for (const lane of this.activeLanes(now)) {
			const reason = this.#sweepReason(lane);
			if (!reason) continue;
			try {
				const outcome = await this.retire(lane.name, reason, { sessionId: lane.sessionId, reason, now });
				if (outcome.retired) retired++;
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_retire_failed name=${lane.name} session=${lane.sessionId} reason=${detail}`);
			}
		}
		return retired;
	}

	/**
	 * Force-retire a lane whose session is provably dead or disowned by the broker.
	 * Unlike normal retire, this skips attempt-state checks and requires liveness
	 * proof that the session is gone. If successful, closes as host_lost and rebinds
	 * the epoch with reason operator_force.
	 */
	forceRetire(name: string): Promise<LaneRetireOutcome> {
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({
				retired: false,
				sessionKey: workSessionKey(name),
				reason: "broker_authority_quarantined",
			});
		if (this.#stopped)
			return Promise.resolve({ retired: false, sessionKey: workSessionKey(name), reason: "gateway is stopping" });
		return this.#forceRetireTask(name);
	}

	#forceRetireTask(name: string, unattended?: { readonly sessionId: string }): Promise<LaneRetireOutcome> {
		const task = (async () => {
			try {
				await this.#recoveryGate?.();
				if (this.#stopped)
					return { retired: false as const, sessionKey: workSessionKey(name), reason: "gateway is stopping" };
				return await this.#forceRetire(name, unattended);
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_force_retire_failed name=${name} reason=${detail}`);
				return { retired: false as const, sessionKey: workSessionKey(name), reason: detail };
			}
		})();
		this.#mutations.add(task);
		void task.then(
			() => this.#mutations.delete(task),
			() => this.#mutations.delete(task),
		);
		return task;
	}

	/**
	 * `unattended` is the dead-lane sweep: it acts only on the session it
	 * confirmed, re-checks the verdict against recent host activity, and settles
	 * the open attempt WITHOUT closing the session or rotating the epoch. The
	 * binding stays, so the next start of the same name resumes the saved
	 * transcript instead of re-reading everything in a cold session; the settled
	 * lane is then reclaimed by admission or the idle sweep like any other.
	 */
	#forceRetire(name: string, unattended?: { readonly sessionId: string }): Promise<LaneRetireOutcome> {
		const sessionKey = workSessionKey(name);
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({ retired: false, sessionKey, reason: "broker_authority_quarantined" });
		// Unlike normal retire, we check the lane exists but do NOT check if an attempt is open.
		// We will only close if liveness proves the session is dead/disowned.
		const preflight = this.activeLanes().find((candidate) => candidate.name === name);
		if (!preflight) return Promise.resolve({ retired: false, sessionKey, reason: "no bound lane for that name" });
		return this.#port.runExclusive(sessionKey, async () => {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
				return { retired: false, sessionKey, reason: "broker_authority_quarantined" };
			if (this.#stopped) return { retired: false, sessionKey, reason: "gateway is stopping" };
			const lane = this.activeLanes().find((candidate) => candidate.name === name);
			if (!lane) return { retired: false, sessionKey, reason: "no bound lane for that name" };
			if (unattended && lane.sessionId !== unattended.sessionId)
				return { retired: false, sessionKey, reason: "lane was rebound since the dead verdict" };
			const job = this.#job(name);
			const record = job === "corrupt" ? undefined : job;
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				if (this.#database.bindings.isBrokerMode()) {
					this.#log(`lane_retire_failed name=${name} session=${lane.sessionId} reason=orphan_no_repo_found`);
					return { retired: false, sessionKey, reason: "orphan lane: no job record and no owned binding repo" };
				}
				repo = process.cwd();
			}
			// For force retire, we must have broker liveness proof that the session is dead or disowned.
			const liveness = await this.#safeLiveness(lane.sessionId, repo);
			// The confirming sweep already saw a repeated dead verdict; this locked
			// re-probe may only veto it. Silence keeps the streak for the next sweep.
			const verdict = unattended ? laneHostVerdict(liveness, this.#now(), lane.lastActivityAt) : undefined;
			if (verdict === "indeterminate") {
				this.#log(`lane_force_retire_rejected name=${name} session=${lane.sessionId} reason=liveness_indeterminate`);
				return { retired: false, sessionKey, reason: "session liveness is indeterminate" };
			}
			if (verdict === "live") this.#deadStreaks.delete(lane.sessionId);
			if (verdict ? verdict !== "dead" : liveness.live !== false && !liveness.disowned) {
				this.#log(`lane_force_retire_rejected name=${name} session=${lane.sessionId} reason=session_still_live`);
				return {
					retired: false,
					sessionKey,
					reason: "session is still live; cannot force retire",
				};
			}
			// Session is confirmed dead or disowned; the attempt owner closes the
			// history and runtime row together before we retire the binding. A
			// completed attempt may also own an unresolved checks cycle that must end
			// when its lane is force-retired.
			const lastAttempt = record?.attempts.at(-1);
			const reason: LaneForceRetireReason = liveness.disowned ? "session_disowned" : "session_dead";
			if (lastAttempt && (!lastAttempt.endedAt || record?.state === "awaiting_checks")) {
				const endedAt = new Date(this.#now()).toISOString();
				if (!this.#forceRetireSettlement?.(name, lastAttempt.opRef, reason, endedAt)) {
					const detail = "work attempt could not be settled";
					this.#log(`lane_force_retire_failed name=${name} opRef=${lastAttempt.opRef} reason=${detail}`);
					return { retired: false, sessionKey, reason: detail };
				}
			} else if (unattended && record) {
				return { retired: false, sessionKey, reason: "attempt already settled" };
			}
			if (unattended && record) {
				this.#deadStreaks.delete(lane.sessionId);
				this.#log(
					`lane_attempt_settled_dead name=${name} session=${lane.sessionId} reason=${reason} binding=kept`,
					"warn",
				);
				return { retired: true, sessionKey, sessionId: lane.sessionId, closed: false, forced: true };
			}
			// Forced retirement is unattended; preserve the durable worktree and
			// branch so its commits and the stored worktree path remain resumable.
			// Close the session and rebind the epoch.
			let closed = false;
			try {
				await this.#port.close({ sessionId: lane.sessionId, repo });
				closed = true;
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=force_close`);
				// For force retire, we proceed even if close fails since we have liveness proof.
			}
			this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, sessionKey));
			this.#log(`lane_retired name=${name} session=${lane.sessionId} reason=operator_force closed=${closed}`);
			return { retired: true, sessionKey, sessionId: lane.sessionId, closed, forced: true };
		});
	}

	/**
	 * Unattended dead-lane reconciliation. A lane is acted on only after its
	 * dead/disowned verdict held on at least two sweeps spanning `deadConfirmMs`,
	 * with no recent host activity; a live answer resets the streak and an
	 * unanswered probe leaves it as is. A lane with an open attempt (or an
	 * unresolved checks cycle) has the attempt settled and keeps its session for
	 * resume; a settled lane is left to admission and the idle sweep; only an
	 * orphan binding without a job record is closed and rebound.
	 * Returns count and names of lanes acted on.
	 */
	async retireAllDead(now = this.#now()): Promise<{ count: number; names: string[] }> {
		if (this.#stopped) return { count: 0, names: [] };
		const retired: string[] = [];
		const lanes = this.activeLanes(now);
		this.#deadStreaks.retain(new Set(lanes.map((lane) => lane.sessionId)));
		for (const lane of lanes) {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(lane.name).jobId)) continue;
			const job = this.#job(lane.name);
			const record = job === "corrupt" ? undefined : job;
			// A settled lane with a job record is reclaimed by admission or the idle
			// sweep; closing it here would only cost its resumable transcript.
			if (record && !lane.attemptOpen && record.state !== "awaiting_checks") {
				this.#deadStreaks.delete(lane.sessionId);
				continue;
			}
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				if (this.#database.bindings.isBrokerMode()) {
					continue; // Skip orphan lanes
				}
				repo = process.cwd();
			}
			const liveness = await this.#safeLiveness(lane.sessionId, repo);
			const streak = this.#deadStreaks.record(lane.sessionId, laneHostVerdict(liveness, now, lane.lastActivityAt), now);
			if (!streak.confirmed) {
				if (streak.first)
					this.#log(
						`lane_dead_suspected name=${lane.name} session=${lane.sessionId} live=${liveness.live ?? "unknown"} disowned=${liveness.disowned}`,
					);
				continue;
			}
			try {
				const outcome = record
					? await this.#forceRetireTask(lane.name, { sessionId: lane.sessionId })
					: await this.forceRetire(lane.name);
				if (outcome.retired) retired.push(lane.name);
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_retire_failed name=${lane.name} session=${lane.sessionId} reason=${detail}`);
			}
		}
		return { count: retired.length, names: retired };
	}

	/** The sweep predicate, evaluated on a fresh lane view; undefined when the lane must stay. */
	#sweepReason(lane: ActiveLane): SweepReason | undefined {
		if (lane.attemptOpen) return undefined;
		if (TERMINAL_JOB_STATES.has(lane.state)) return `job_${lane.state}` as SweepReason;
		return lane.idleMs >= this.idleRetireMs ? "idle" : undefined;
	}

	/**
	 * Runtime terminality for the ledger's last attempt. `endedAt` is the
	 * gateway's observation, not the broker's: `attempt_ended` (reaped wait)
	 * and `terminal_uncertain` (crash-left) attempts may still be running. They
	 * are settled only when `status` reports a terminal op, or reports an
	 * unknown op on a session the broker says is dead (nothing can be running).
	 * Status errors (e.g. session_unavailable) are also treated as proof the
	 * session is gone: consult liveness and settle if dead/disowned.
	 */
	async #runtimeSettled(
		job: LaneJobRecord | undefined,
		sessionId: string,
		repo: string,
	): Promise<{ readonly settled: true } | { readonly settled: false; readonly reason: string }> {
		const last = job?.attempts.at(-1);
		if (!last || last.sessionId !== sessionId || !UNPROVEN_END_STATES.has(last.endState ?? ""))
			return { settled: true };
		let status: string;
		let statusError: string | undefined;
		try {
			status = (await this.#port.status({ sessionId, repo, opRef: last.opRef })).status.status;
		} catch (error) {
			// Status unavailable (e.g., session_unavailable) means the broker says
			// the session is gone. Treat it the same as unknown: check liveness.
			statusError = sanitizeDiagnostic(diagnostic(error));
			status = "unknown";
		}
		if (status === "terminal_ok" || status === "failed") return { settled: true };
		if (status === "unknown") {
			const liveness = await this.#liveness(sessionId, repo);
			if (liveness.live === false || liveness.disowned) return { settled: true };
		}
		const detail = statusError ? `status error: ${statusError}` : status;
		return {
			settled: false,
			reason: `attempt ${last.opRef} ended ${last.endState} in the ledger but the broker reports ${detail}`,
		};
	}

	async #liveness(sessionId: string, repo: string): Promise<{ live: boolean | undefined; disowned: boolean }> {
		if (!this.#port.liveness) return { live: undefined, disowned: false };
		return await this.#port.liveness({ sessionId, repo });
	}

	/**
	 * Safe liveness probe: catches errors and logs them instead of throwing.
	 * Errors are conservative: assume session is still alive (live: undefined).
	 */
	async #safeLiveness(sessionId: string, repo: string): Promise<{ live: boolean | undefined; disowned: boolean }> {
		try {
			return await this.#liveness(sessionId, repo);
		} catch (error) {
			const detail = sanitizeDiagnostic(diagnostic(error));
			this.#log(`liveness_probe_failed session=${sessionId} repo=${repo} reason=${detail}`);
			// Conservative: assume session still alive if we can't probe.
			return { live: undefined, disowned: false };
		}
	}

	/** Cleans up a runtime-created worktree and its branch. Throws on error (issue #480). */
	async #cleanupWorktree(
		name: string,
		sessionId: string,
		worktreePath: string,
		repository: string,
		removeRepository: boolean,
	): Promise<void> {
		if (removeRepository) assertManagedRepositoryPath(this.#laneRoot, name, repository);

		// A previous cleanup can have removed the managed bare repository before
		// the binding epoch was rotated. Treat that state as already cleaned up.
		if (removeRepository && !existsSync(repository)) {
			if (existsSync(worktreePath)) throw new Error("managed repository is missing while the worktree still exists");
			return;
		}
		// Remove the worktree using git worktree remove --force
		const removeResult = await runGit(["worktree", "remove", "--force", worktreePath], { cwd: repository });

		if (removeResult.exitCode !== 0) {
			const stderr = removeResult.stderr.trim() || "unknown error";
			if (existsSync(worktreePath)) throw new Error(`git worktree remove failed: ${stderr}`);

			// External deletion leaves a stale administrative worktree record. Prune
			// only after confirming the path is absent, then verify it is unregistered.
			const pruneResult = await runGit(["worktree", "prune"], { cwd: repository });
			if (pruneResult.exitCode !== 0) {
				const pruneError = pruneResult.stderr.trim() || "unknown error";
				throw new Error(`git worktree prune failed: ${pruneError}`);
			}
			const listResult = await runGit(["worktree", "list", "--porcelain"], { cwd: repository });
			if (listResult.exitCode !== 0) {
				const listError = listResult.stderr.trim() || "unknown error";
				throw new Error(`git worktree list failed: ${listError}`);
			}
			const canonicalWorktreePath = canonicalPathIncludingMissing(worktreePath);
			const stillRegistered = listResult.stdout.split(/\r?\n/).some((line) => {
				if (!line.startsWith("worktree ")) return false;
				try {
					return canonicalPathIncludingMissing(line.slice("worktree ".length)) === canonicalWorktreePath;
				} catch {
					// If a listed path cannot be canonicalized, do not delete the repository.
					return true;
				}
			});
			if (stillRegistered) throw new Error(`worktree remains registered after pruning: ${stderr}`);
		}

		// Delete the branch using git branch -D
		const branchName = managedLaneBranchName(name);
		// Branch deletion failure (including its deadline) is non-fatal; only log it
		const branchResult = await runGit(["branch", "-D", branchName], { cwd: repository }).catch((error: unknown) => ({
			exitCode: -1,
			stdout: "",
			stderr: error instanceof Error ? error.message : String(error),
		}));
		if (branchResult.exitCode !== 0) {
			const stderr = branchResult.stderr.trim() || "unknown error";
			this.#log(
				`lane_branch_delete_failed name=${name} session=${sessionId} branch=${branchName} detail=${stderr}`,
				"warn",
			);
		}
		if (removeRepository) rmSync(repository, { recursive: true, force: true });
	}

	/** `undefined` when no job row exists; `"corrupt"` when one exists but cannot be trusted. */
	#job(name: string): LaneJobRecord | "corrupt" | undefined {
		const json = this.#database.work.laneJobJson(laneJobIdentity(name).jobId);
		// A row that exists with an empty body is damage, not absence.
		if (json === undefined) return undefined;
		try {
			return parseLaneJobRecord(json);
		} catch {
			return "corrupt";
		}
	}
}

function assertManagedRepositoryPath(laneRoot: string | undefined, name: string, repository: string): void {
	if (laneRoot === undefined) throw new Error("configured lane root is unavailable for managed repository cleanup");
	const expectedPath = managedLaneRepositoryPath(laneRoot, name);
	if (resolve(repository) !== expectedPath) throw new Error("managed repository path does not match its lane name");
	if (!existsSync(repository)) return;

	const repositoryStat = lstatSync(repository);
	if (!repositoryStat.isDirectory() || repositoryStat.isSymbolicLink())
		throw new Error("managed repository path is not a real directory");
	const canonicalLaneRoot = realpathSync(laneRoot);
	const canonicalRepositoryRoot = realpathSync(join(laneRoot, "repositories"));
	const canonicalRepository = realpathSync(repository);
	const expectedCanonicalPath = join(canonicalRepositoryRoot, Buffer.from(name, "utf8").toString("hex"));
	const laneRootPrefix = canonicalLaneRoot.endsWith(sep) ? canonicalLaneRoot : `${canonicalLaneRoot}${sep}`;
	const repositoryRootPrefix = canonicalRepositoryRoot.endsWith(sep)
		? canonicalRepositoryRoot
		: `${canonicalRepositoryRoot}${sep}`;
	if (
		!canonicalRepository.startsWith(laneRootPrefix) ||
		!canonicalRepositoryRoot.startsWith(laneRootPrefix) ||
		canonicalRepository !== expectedCanonicalPath ||
		!canonicalRepository.startsWith(repositoryRootPrefix)
	)
		throw new Error("managed repository path escapes its configured lane root");
}

function canonicalPathIncludingMissing(target: string): string {
	let current = resolve(target);
	const missing: string[] = [];
	while (true) {
		try {
			return resolve(realpathSync(current), ...missing.reverse());
		} catch (error) {
			const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
			if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
			const parent = dirname(current);
			if (parent === current) throw error;
			missing.push(basename(current));
			current = parent;
		}
	}
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	if (value === undefined) return fallback;
	if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
	return value;
}

function unsettled(sessionKey: string, lane: ActiveLane): LaneRetireOutcome {
	return {
		retired: false,
		sessionKey,
		reason:
			lane.state === "corrupt"
				? `lane job record for ${sessionKey} is corrupt; reconcile it before retiring`
				: `attempt still open on ${sessionKey}; let it end first`,
	};
}

function diagnostic(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
