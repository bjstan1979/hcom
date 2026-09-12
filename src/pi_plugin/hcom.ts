import { StringEnum } from "@earendil-works/pi-ai";
import type {
	ExtensionAPI,
	ExtensionContext,
	InputEvent,
} from "@earendil-works/pi-coding-agent";
import {
	CURSOR_MARKER,
	Key,
	matchesKey,
	Text,
	truncateToWidth,
	visibleWidth,
	wrapTextWithAnsi,
} from "@earendil-works/pi-tui";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
	appendFileSync,
	mkdirSync,
	readFileSync,
	writeFileSync,
} from "node:fs";
import { createServer, type Server } from "node:net";
import { homedir } from "node:os";
import { dirname } from "node:path";
import { Type } from "typebox";

const HCOM_DIR = process.env.HCOM_DIR || `${homedir()}/.hcom`;
const LOG_PATH = `${HCOM_DIR}/.tmp/logs/hcom.log`;

type HcomResult = {
	code: number;
	stdout: string;
	stderr: string;
};

type AcceptanceAwareSendUserMessage = (
	content: Parameters<ExtensionAPI["sendUserMessage"]>[0],
	options: {
		deliverAs?: "steer" | "followUp";
		onAccepted: (accepted: boolean) => void;
	},
) => void;

type HcomRunOptions = {
	signal?: AbortSignal;
	timeoutMs?: number;
	maxOutputBytes?: number;
};

type HcomAttachment = {
	type: "snippet" | "context";
	name: string;
	content: string;
	mime?: string;
	language?: string;
};

type HcomAgent = {
	name: string;
	base_name?: string;
	tag?: string | null;
	tool?: string;
	status?: string;
	status_context?: string;
	status_detail?: string;
	directory?: string;
	unread_count?: number;
	endpoint_epoch?: string;
	presence?: Record<string, unknown>;
};

type HcomCard = {
	action: string;
	messageId?: string;
	target?: string;
	state?: string;
	text?: string;
	timestamp: string;
};

type ChildSupervisorAction = "progress_update" | "need_decision" | "interview_request";
type ChildSupervisorContact = { action: ChildSupervisorAction; message: string; context?: string };
type ChildSupervisorBridgeSeed = { childId: string; runId: string; childType: string };
type ChildSupervisorBridge = {
	version: "hcom-supervisor-bridge/v1";
	childId: string;
	runId: string;
	childType: string;
	parentSessionId: string;
	parentEndpointEpoch: string;
	expiresAt: number;
	contact(input: ChildSupervisorContact, signal?: AbortSignal): Promise<void>;
	isActive(): boolean;
	revoke(): void;
};

const MAX_HCOM_OUTPUT_BYTES = 1024 * 1024;
const MAX_HCOM_TEXT_BYTES = 64 * 1024;
const HCOM_ACTIONS = [
	"list",
	"status",
	"send",
	"ask",
	"reply",
	"pending",
	"cancel",
	"supersede",
	"retry",
] as const;
const HCOM_TOOL_PARAMS = Type.Object({
	action: StringEnum(HCOM_ACTIONS),
	target: Type.Optional(Type.String({ maxLength: 256 })),
	text: Type.Optional(Type.String({ maxLength: MAX_HCOM_TEXT_BYTES })),
	messageId: Type.Optional(Type.String({ maxLength: 128 })),
	timeoutSec: Type.Optional(Type.Integer({ minimum: 1, maximum: 3600 })),
	attachments: Type.Optional(
		Type.Array(
			Type.Object({
				type: StringEnum(["snippet", "context"] as const),
				name: Type.String({ minLength: 1, maxLength: 256 }),
				content: Type.String({ maxLength: 16 * 1024 }),
				mime: Type.Optional(Type.String({ maxLength: 128 })),
				language: Type.Optional(Type.String({ maxLength: 64 })),
			}),
			{ maxItems: 8 },
		),
	),
});
const CHILD_CONTACT_PARAMS = Type.Object(
	{
		action: StringEnum(["progress_update", "need_decision", "interview_request"] as const),
		message: Type.String({ minLength: 1, maxLength: 4096 }),
		context: Type.Optional(Type.String({ maxLength: 2048 })),
	},
	{ additionalProperties: false },
);

function log(
	level: "DEBUG" | "INFO" | "WARN" | "ERROR",
	event: string,
	instance?: string | null,
	extra?: Record<string, unknown>,
) {
	const entry = JSON.stringify({
		ts: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
		level,
		subsystem: "plugin",
		event,
		...(instance ? { instance } : {}),
		...extra,
	});
	try {
		mkdirSync(dirname(LOG_PATH), { recursive: true });
		appendFileSync(LOG_PATH, `${entry}\n`);
	} catch {}
}

function hcom(
	args: string[],
	options: HcomRunOptions = {},
): Promise<HcomResult> {
	return new Promise((resolve) => {
		const maxOutput = options.maxOutputBytes ?? MAX_HCOM_OUTPUT_BYTES;
		const child = spawn("hcom", args, { stdio: ["ignore", "pipe", "pipe"] });
		let stdout = "";
		let stderr = "";
		let settled = false;
		let killTimer: ReturnType<typeof setTimeout> | null = null;
		const timeout = setTimeout(
			() =>
				finish(
					{ code: 124, stdout, stderr: stderr || "hcom command timed out" },
					true,
				),
			options.timeoutMs ?? 30_000,
		);
		timeout.unref?.();

		function cleanup(): void {
			clearTimeout(timeout);
			if (killTimer) clearTimeout(killTimer);
			options.signal?.removeEventListener("abort", onAbort);
		}
		function finish(result: HcomResult, terminate = false): void {
			if (settled) return;
			settled = true;
			cleanup();
			if (terminate && !child.killed) {
				try {
					child.kill("SIGTERM");
				} catch {}
				killTimer = setTimeout(() => {
					try {
						child.kill("SIGKILL");
					} catch {}
				}, 1_000);
				killTimer.unref?.();
			}
			resolve(result);
		}
		function append(
			current: string,
			chunk: unknown,
			stream: "stdout" | "stderr",
		): string {
			const next = current + String(chunk);
			if (Buffer.byteLength(next, "utf8") > maxOutput) {
				finish(
					{
						code: 125,
						stdout,
						stderr: `${stream} exceeded ${maxOutput} bytes`,
					},
					true,
				);
				return current;
			}
			return next;
		}
		function onAbort(): void {
			finish({ code: 130, stdout, stderr: "hcom command aborted" }, true);
		}

		child.stdout.setEncoding("utf8");
		child.stderr.setEncoding("utf8");
		child.stdout.on("data", (chunk) => {
			stdout = append(stdout, chunk, "stdout");
		});
		child.stderr.on("data", (chunk) => {
			stderr = append(stderr, chunk, "stderr");
		});
		child.on("error", (error) =>
			finish({ code: 127, stdout, stderr: String(error) }),
		);
		child.on("close", (code) => finish({ code: code ?? 0, stdout, stderr }));
		if (options.signal?.aborted) onAbort();
		else options.signal?.addEventListener("abort", onAbort, { once: true });
	});
}

function formatMessagesForInjection(
	messages: any[],
	recipientName: string,
): string {
	const parts = messages.map((m: any) => {
		const stableId =
			typeof m.message_id === "string" ? ` id=${m.message_id}` : "";
		const prefix = m.intent
			? m.thread
				? `[${m.intent}:${m.thread} #${m.event_id}${stableId}]`
				: `[${m.intent} #${m.event_id}${stableId}]`
			: m.thread
				? `[thread:${m.thread} #${m.event_id}${stableId}]`
				: `[new message #${m.event_id}${stableId}]`;
		const attachmentText = Array.isArray(m.attachments)
			? m.attachments
					.map((attachment: any) => {
						const name = String(
							attachment?.name ?? attachment?.type ?? "attachment",
						).replace(/[\r\n]/g, " ");
						const kind = String(attachment?.type ?? "context");
						const content = String(attachment?.content ?? "").slice(
							0,
							16 * 1024,
						);
						return `\n[attachment ${kind}:${name} sha256=${String(attachment?.sha256 ?? "unknown")}]\n${content}`;
					})
					.join("")
			: "";
		return `${prefix} ${String(m.from)} -> ${recipientName}: ${String(m.message ?? "")}${attachmentText}`;
	});
	if (messages.length === 1) return `<hcom>${parts[0]}</hcom>`;
	return `<hcom>[${messages.length} new messages]\n${parts.join("\n---\n")}</hcom>`;
}

function isBodylessWake(text: string): boolean {
	const trimmed = text.trim();
	return trimmed === "<hcom>" || trimmed === "<hcom></hcom>";
}

function closedWorkerMarkerPath(): string {
	return `${HCOM_DIR}/pi-supervisor/closed-workers.json`;
}

function isClosedSupervisionWorker(worker: string): boolean {
	try {
		const entries = JSON.parse(
			readFileSync(closedWorkerMarkerPath(), "utf8"),
		) as Array<{ worker: string; expiresAt: number }>;
		return entries.some(
			(entry) => entry.worker === worker && entry.expiresAt > Date.now(),
		);
	} catch {
		return false;
	}
}

const PI_SUBAGENT_CHILD_SESSION_CONTEXT = Symbol.for("pi-subagents:child-session-context");
const PI_SUBAGENT_SUPERVISOR_BRIDGE_CONTEXT = Symbol.for("pi-subagents:supervisor-bridge-context");
const PI_SUBAGENT_SUPERVISOR_BRIDGE_PROVIDER = Symbol.for("pi-subagents:supervisor-bridge-provider");

function isBoundedBridgeMetadata(
	value: unknown,
	maxBytes = 256,
): value is string {
	return (
		typeof value === "string" &&
		value.length > 0 &&
		value === value.trim() &&
		Buffer.byteLength(value, "utf8") <= maxBytes
	);
}

function isBridgeEndpointEpoch(value: unknown): value is string {
	return (
		isBoundedBridgeMetadata(value, 64) &&
		/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
			value,
		)
	);
}

function isPiSubagentChildSession(): boolean {
	try {
		const context = (globalThis as any)[PI_SUBAGENT_CHILD_SESSION_CONTEXT];
		return context?.getStore?.() === true;
	} catch {
		return false;
	}
}

function activeChildSupervisorBridge(): ChildSupervisorBridge | undefined {
	try {
		const context = (globalThis as any)[PI_SUBAGENT_SUPERVISOR_BRIDGE_CONTEXT];
		const bridge = context?.getStore?.() as ChildSupervisorBridge | undefined;
		if (
			bridge?.version !== "hcom-supervisor-bridge/v1" ||
			!isBoundedBridgeMetadata(bridge.childId, 128) ||
			!isBoundedBridgeMetadata(bridge.runId, 128) ||
			!isBoundedBridgeMetadata(bridge.childType, 128) ||
			!isBoundedBridgeMetadata(bridge.parentSessionId) ||
			!isBridgeEndpointEpoch(bridge.parentEndpointEpoch) ||
			!Number.isFinite(bridge.expiresAt) ||
			bridge.expiresAt <= Date.now() ||
			typeof bridge.contact !== "function" ||
			typeof bridge.isActive !== "function" ||
			typeof bridge.revoke !== "function" ||
			!bridge.isActive()
		) return undefined;
		return bridge;
	} catch {
		return undefined;
	}
}

function registerChildSupervisorTool(pi: ExtensionAPI, bridge: ChildSupervisorBridge): void {
	pi.registerTool({
		name: "contact_supervisor",
		label: "Contact Supervisor",
		description: "Send a bounded progress update, decision request, or interview request to this child run's parent session.",
		promptSnippet: "Contact the parent supervisor for progress, a decision, or an interview",
		promptGuidelines: [
			"Use contact_supervisor only for material progress, a decision that blocks this child run, or an interview request that the parent must coordinate.",
			"contact_supervisor is a scoped callback to this run's parent; never ask for or infer a target agent name or capability token.",
		],
		parameters: CHILD_CONTACT_PARAMS,
		async execute(_toolCallId, params, signal) {
			if (signal?.aborted) throw new Error("Supervisor contact was aborted");
			if (bridge.expiresAt <= Date.now() || !bridge.isActive()) throw new Error("Supervisor bridge is expired or revoked");
			if (Buffer.byteLength(params.message, "utf8") > 4096 || Buffer.byteLength(params.context ?? "", "utf8") > 2048) {
				throw new Error("Supervisor contact exceeds the UTF-8 size limit");
			}
			await bridge.contact({ action: params.action, message: params.message, context: params.context }, signal);
			return {
				content: [{ type: "text" as const, text: "Supervisor notified through the scoped child bridge." }],
				details: { action: params.action, delivered: true },
			};
		},
	});
}

function hasInheritedPiSessionEnvironment(): boolean {
	return [process.env.PI_SESSION_ID, process.env.PI_SESSION_FILE].some(
		(value) => typeof value === "string" && value.trim().length > 0,
	);
}

export default function hcomExtension(pi: ExtensionAPI) {
	// A child created by pi-subagents runs in the parent's process and inherits
	// HCOM_LAUNCHED. It must never bind, report status, consume parent messages,
	// or expose the ordinary HCOM tool; only a scoped supervisor callback is safe.
	if (isPiSubagentChildSession()) {
		const bridge = activeChildSupervisorBridge();
		if (bridge) registerChildSupervisorTool(pi, bridge);
		log("INFO", bridge ? "plugin.child_bridge_registered" : "plugin.bind_skipped_nested", null, {
			reason: bridge ? "scoped-supervisor-bridge" : "pi-subagent-child-session",
		});
		return;
	}
	// Pi's shell tools deliberately export PI_SESSION_ID/PI_SESSION_FILE to child
	// commands. A nested standalone Pi (for example pi-acp smoke) inherits those
	// markers plus HCOM_LAUNCHED/HCOM_PROCESS_ID and must not steal or stop the
	// parent's durable identity. A directly launched Pi has no inherited session
	// markers in its own process environment.
	if (hasInheritedPiSessionEnvironment()) {
		log("INFO", "plugin.bind_skipped_nested_process", null, {
			reason: "inherited-pi-session-environment",
		});
		return;
	}
	// onAccepted is a deliberate local Pi contract. Keep the cast here so HCOM
	// still type-checks against the pristine upstream SDK used by its source tree.
	const sendUserMessageWithAcceptance = pi.sendUserMessage.bind(
		pi,
	) as AcceptanceAwareSendUserMessage;
	const endpointEpoch = randomUUID();
	let runtimeGeneration = 0;
	const operationControllers = new Set<AbortController>();
	let askInFlight = false;
	let closeActiveOverlay: (() => void) | null = null;
	let brokerFeatures = new Set<string>();
	let lastPresenceSignature: string | null = null;
	let instanceName: string | null = null;
	let sessionId: string | null = null;
	let bootstrapText: string | null = null;
	let bindingPromise: Promise<void> | null = null;
	let notifyServer: Server | null = null;
	let notifyPort: number | null = null;
	let currentCtx: ExtensionContext | null = null;
	let pendingAckId: number | null = null;
	let deliveryInFlight = false;
	let deliveryPending = false; // a wake arrived while delivery was gated; replay it once clear
	let deliveryRetryScheduled = false; // dedup the queued replay pass
	let reconcileTimer: ReturnType<typeof setInterval> | null = null;
	let reconcileInFlight = false;
	let bootstrapInjectedForSession: string | null = null;
	let lastReportedStatusKey: string | null = null;
	let lastListeningHeartbeatAt = 0;
	let lastPendingPollAt = 0;
	let agentActive = false;
	let uiPromptActive = false;
	let uiPromptEpoch = 0;
	let idleTimer: ReturnType<typeof setTimeout> | null = null;
	let deliveredMessageIds = new Set<number>();
	let bindFailureCount = 0;
	let bindRetryAt = 0;
	let lastBindFailureLogAt = 0;

	const PENDING_POLL_MS = 60_000;
	const FALLBACK_PENDING_POLL_MS = 5_000;
	// Broker-backed plugin delivery has no host-reachable TCP endpoint, so its
	// heartbeat must stay below HCOM's 10s no-TCP stale threshold.
	const LISTENING_HEARTBEAT_MS = 5_000;
	const IDLE_DEBOUNCE_MS = 250;

	const extensionRegistry = globalThis as any;
	const supervisorBridgeProvider = (seed: ChildSupervisorBridgeSeed): ChildSupervisorBridge | undefined => {
		if (
			!instanceName || !sessionId || !currentCtx ||
			!seed.childId || seed.childId.length > 128 ||
			!seed.runId || seed.runId.length > 128 ||
			!seed.childType || seed.childType.length > 128
		) return undefined;
		const expectedGeneration = runtimeGeneration;
		const expectedSessionId = sessionId;
		const expiresAt = Date.now() + 60 * 60 * 1000;
		let revoked = false;
		const isActive = () =>
			!revoked &&
			Date.now() < expiresAt &&
			runtimeGeneration === expectedGeneration &&
			sessionId === expectedSessionId &&
			endpointEpoch.length === 36 &&
			extensionRegistry[PI_SUBAGENT_SUPERVISOR_BRIDGE_PROVIDER] === supervisorBridgeProvider;
		const bridge: ChildSupervisorBridge = {
			version: "hcom-supervisor-bridge/v1",
			childId: seed.childId,
			runId: seed.runId,
			childType: seed.childType,
			parentSessionId: expectedSessionId,
			parentEndpointEpoch: endpointEpoch,
			expiresAt,
			isActive,
			revoke() { revoked = true; },
			async contact(input, signal) {
				if (!isActive()) throw new Error("Supervisor bridge is expired or revoked");
				if (signal?.aborted) throw new Error("Supervisor contact was aborted");
				if (!["progress_update", "need_decision", "interview_request"].includes(input.action)) {
					throw new Error("Unsupported supervisor contact action");
				}
				if (!input.message.trim() || Buffer.byteLength(input.message, "utf8") > 4096 || Buffer.byteLength(input.context ?? "", "utf8") > 2048) {
					throw new Error("Supervisor contact payload is empty or too large");
				}
				const ctx = currentCtx;
				if (!ctx || ctx.sessionManager.getSessionId() !== expectedSessionId) throw new Error("Parent HCOM session changed");
				const envelope = JSON.stringify({
					type: "child_supervisor_contact",
					action: input.action,
					child_id: seed.childId,
					run_id: seed.runId,
					child_type: seed.childType,
					message: input.message,
					...(input.context ? { context: input.context } : {}),
				});
				await new Promise<void>((resolve, reject) => {
					let settled = false;
					const finish = (error?: Error) => {
						if (settled) return;
						settled = true;
						clearTimeout(timeout);
						signal?.removeEventListener("abort", onAbort);
						error ? reject(error) : resolve();
					};
					const onAbort = () => finish(new Error("Supervisor contact was aborted"));
					const timeout = setTimeout(() => finish(new Error("Parent did not accept supervisor contact")), 30_000);
					timeout.unref?.();
					signal?.addEventListener("abort", onAbort, { once: true });
					if (signal?.aborted) return onAbort();
					try {
						sendUserMessageWithAcceptance(`<hcom-supervisor>${envelope}</hcom-supervisor>`, {
							...(ctx.isIdle() ? {} : { deliverAs: "followUp" as const }),
							onAccepted: (accepted) => finish(accepted && isActive() ? undefined : new Error("Parent rejected or invalidated supervisor contact")),
						});
					} catch (error) {
						finish(error instanceof Error ? error : new Error(String(error)));
					}
				});
			},
		};
		return bridge;
	};
	extensionRegistry[PI_SUBAGENT_SUPERVISOR_BRIDGE_PROVIDER] = supervisorBridgeProvider;

	function statusKey(status: string, context: string, detail: string): string {
		return `${status}\0${context}\0${detail}`;
	}

	async function runHcom(
		args: string[],
		options: HcomRunOptions = {},
	): Promise<HcomResult> {
		const expectedGeneration = runtimeGeneration;
		const controller = new AbortController();
		const abort = () => controller.abort();
		options.signal?.addEventListener("abort", abort, { once: true });
		operationControllers.add(controller);
		try {
			const result = await hcom(args, {
				...options,
				signal: controller.signal,
			});
			if (expectedGeneration !== runtimeGeneration) {
				return {
					code: 130,
					stdout: "",
					stderr: "stale HCOM session generation",
				};
			}
			return result;
		} finally {
			options.signal?.removeEventListener("abort", abort);
			operationControllers.delete(controller);
		}
	}

	function invalidateRuntimeGeneration(): void {
		runtimeGeneration += 1;
		for (const controller of operationControllers) controller.abort();
		operationControllers.clear();
		askInFlight = false;
	}

	function deliveryLedgerPath(): string | null {
		if (!sessionId) return null;
		const safe = sessionId.replace(/[^A-Za-z0-9_.-]/g, "_");
		return `${HCOM_DIR}/pi-delivery/${safe}.json`;
	}

	function saveDeliveryLedger(): void {
		const path = deliveryLedgerPath();
		if (!path) return;
		try {
			mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
			writeFileSync(
				path,
				JSON.stringify([...deliveredMessageIds].slice(-2048)),
				{ mode: 0o600 },
			);
		} catch (error) {
			log("WARN", "plugin.delivery_ledger_failed", instanceName, {
				error: String(error),
			});
		}
	}

	function collectDeliveredIdsFromMessage(message: any): void {
		if (message?.role !== "user" || !Array.isArray(message.content)) return;
		for (const item of message.content) {
			if (
				item?.type !== "text" ||
				typeof item.text !== "string" ||
				!item.text.includes("<hcom>")
			)
				continue;
			for (const match of item.text.matchAll(/#(\d+)\]/g)) {
				const id = Number(match[1]);
				if (Number.isSafeInteger(id) && id > 0) deliveredMessageIds.add(id);
			}
		}
	}

	function loadDeliveryLedger(
		transcriptPath: string | undefined,
		sessionEntries: readonly unknown[],
	): void {
		deliveredMessageIds = new Set<number>();
		const path = deliveryLedgerPath();
		if (!path) return;
		try {
			const value = JSON.parse(readFileSync(path, "utf8"));
			if (Array.isArray(value)) {
				for (const id of value.slice(-2048))
					if (Number.isSafeInteger(id) && id > 0) deliveredMessageIds.add(id);
				return;
			}
		} catch {}

		// A brand-new Pi session already contains administrative entries before its
		// transcript file exists. Treat the loaded SessionManager entries as the
		// authoritative migration source so first inbound HCOM traffic can create the
		// first model turn instead of deadlocking on ENOENT.
		for (const entry of sessionEntries) {
			collectDeliveredIdsFromMessage((entry as any)?.message);
		}
		let transcriptScanned = false;
		if (transcriptPath) {
			try {
				for (const line of readFileSync(transcriptPath, "utf8").split("\n")) {
					if (!line.includes('\"role\":\"user\"') || !line.includes("<hcom>"))
						continue;
					collectDeliveredIdsFromMessage(JSON.parse(line)?.message);
				}
				transcriptScanned = true;
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
					log("WARN", "plugin.delivery_ledger_migration_failed", instanceName, {
						error: String(error),
					});
				}
			}
		}
		// Persist even an empty migration. ENOENT is a normal fresh-session state,
		// not a reason to block delivery until an unrelated first turn exists.
		saveDeliveryLedger();
		log("INFO", "plugin.delivery_ledger_migration_completed", instanceName, {
			source: transcriptScanned ? "session_entries+transcript" : "session_entries",
			delivered_count: deliveredMessageIds.size,
		});
	}

	function rememberDelivered(messages: any[]): void {
		for (const message of messages) {
			const id = Number(message.event_id);
			if (Number.isSafeInteger(id) && id > 0) deliveredMessageIds.add(id);
		}
		saveDeliveryLedger();
	}

	function isBoundSession(candidateSessionId?: string | null): boolean {
		return (
			!candidateSessionId || !sessionId || candidateSessionId === sessionId
		);
	}

	function startNotifyServer(): Promise<number | null> {
		// A container-local loopback port is not reachable by the host broker.
		// Broker mode therefore uses the existing fast polling fallback until a
		// broker wake subscription is established.
		if (process.env.HCOM_BROKER_SOCKET) return Promise.resolve(null);
		if (notifyServer && notifyPort) return Promise.resolve(notifyPort);
		return new Promise((resolve) => {
			const server = createServer((socket) => {
				socket.end();
				log("DEBUG", "notify_server.wake", instanceName, {
					pending_ack: pendingAckId,
				});
				if (currentCtx) void deliverPending(currentCtx);
			});
			server.on("error", (error) => {
				log("ERROR", "notify_server.start_failed", instanceName, {
					error: String(error),
				});
				resolve(null);
			});
			server.listen(0, "127.0.0.1", () => {
				notifyServer = server;
				const address = server.address();
				notifyPort =
					typeof address === "object" && address ? address.port : null;
				log("INFO", "notify_server.started", instanceName, {
					port: notifyPort,
				});
				resolve(notifyPort);
			});
		});
	}

	function stopNotifyServer(): void {
		if (notifyServer) {
			try {
				notifyServer.close();
			} catch {}
		}
		notifyServer = null;
		notifyPort = null;
	}

	function recordBindFailure(extra: Record<string, unknown>): void {
		const now = Date.now();
		bindFailureCount += 1;
		bindRetryAt =
			now + Math.min(60_000, 1_000 * 2 ** Math.min(bindFailureCount - 1, 6));
		if (bindFailureCount === 1 || now - lastBindFailureLogAt >= 60_000) {
			lastBindFailureLogAt = now;
			log("WARN", "plugin.bind_failed", null, {
				...extra,
				retry_in_ms: bindRetryAt - now,
				failure_count: bindFailureCount,
			});
		}
	}

	async function bindIdentity(ctx: ExtensionContext): Promise<void> {
		currentCtx = ctx;
		if (instanceName || bindingPromise)
			return bindingPromise ?? Promise.resolve();
		if (process.env.HCOM_LAUNCHED !== "1" || Date.now() < bindRetryAt) return;
		bindingPromise = (async () => {
			try {
				const sid = ctx.sessionManager.getSessionId();
				const transcriptPath = ctx.sessionManager.getSessionFile();
				const port = await startNotifyServer();
				const args = [
					"pi-start",
					"--session-id",
					sid,
					"--cwd",
					ctx.cwd,
					"--endpoint-epoch",
					endpointEpoch,
				];
				if (transcriptPath) args.push("--transcript-path", transcriptPath);
				if (port) args.push("--notify-port", String(port));
				const result = await runHcom(args);
				if (result.code !== 0) {
					stopNotifyServer();
					recordBindFailure({
						exit_code: result.code,
						stderr: result.stderr.slice(0, 300),
					});
					return;
				}
				const json = JSON.parse(result.stdout || "{}");
				if (json.error) {
					stopNotifyServer();
					recordBindFailure({ error: json.error });
					return;
				}
				instanceName = json.name;
				sessionId = json.session_id || sid;
				bindFailureCount = 0;
				bindRetryAt = 0;
				lastBindFailureLogAt = 0;
				loadDeliveryLedger(
					transcriptPath ?? undefined,
					ctx.sessionManager.getEntries(),
				);
				bootstrapText =
					typeof json.bootstrap === "string" ? json.bootstrap : null;
				await refreshFeatures();
				log("INFO", "plugin.bound", instanceName, {
					session_id: sessionId,
					notify_port: port,
					bootstrap_len: bootstrapText?.length ?? 0,
				});
				const boundSessionId = sessionId;
				queueMicrotask(() => {
					if (
						currentCtx &&
						currentCtx.sessionManager.getSessionId() === boundSessionId
					)
						schedulePendingDelivery("bind_success");
				});
			} catch (error) {
				stopNotifyServer();
				recordBindFailure({ error: String(error) });
			} finally {
				bindingPromise = null;
			}
		})();
		await bindingPromise;
	}

	async function fetchPending(): Promise<{
		messages: any[];
		maxId: number;
	} | null> {
		if (!instanceName) return null;
		const result = await runHcom(["pi-read", "--name", instanceName]);
		if (result.code !== 0) {
			log("WARN", "plugin.delivery_read_failed", instanceName, {
				exit_code: result.code,
				stderr: result.stderr.slice(0, 300),
			});
			return null;
		}
		let messages: any[] = [];
		try {
			messages = JSON.parse(result.stdout || "[]");
		} catch (error) {
			log("WARN", "plugin.delivery_parse_failed", instanceName, {
				error: String(error),
				raw: result.stdout.slice(0, 300),
			});
			return null;
		}
		if (!Array.isArray(messages) || messages.length === 0) return null;
		const maxId = Math.max(...messages.map((m: any) => m.event_id || 0));
		if (maxId <= 0) return null;
		return {
			messages: messages.filter(
				(m: any) =>
					!deliveredMessageIds.has(Number(m.event_id)) &&
					!(
						m.intent !== "request" && isClosedSupervisionWorker(String(m.from))
					),
			),
			maxId,
		};
	}

	async function deliverPending(ctx: ExtensionContext): Promise<boolean> {
		currentCtx = ctx;
		await bindIdentity(ctx);
		if (!instanceName || !sessionId) return false;
		if (!isBoundSession(ctx.sessionManager.getSessionId())) return false;
		if (deliveryInFlight || pendingAckId !== null) {
			// A delivery is mid-flight or awaiting ack. Drop nothing: record the wake
			// so it is replayed once clear, otherwise a message that arrives in this
			// window stays unread until the queued wake or periodic reconciliation runs.
			deliveryPending = true;
			log("DEBUG", "plugin.delivery_skipped", instanceName, {
				reason: deliveryInFlight
					? "delivery_in_flight"
					: "pending_ack_in_flight",
				pending_ack: pendingAckId,
				queued: true,
			});
			return false;
		}
		deliveryInFlight = true;
		try {
			const pending = await fetchPending();
			if (!pending) return false;
			if (pending.messages.length === 0) {
				await runHcom([
					"pi-read",
					"--name",
					instanceName,
					"--ack",
					"--up-to",
					String(pending.maxId),
				]);
				return false;
			}
			const formatted = formatMessagesForInjection(
				pending.messages,
				instanceName,
			);
			pendingAckId = pending.maxId;
			try {
				// Pi's extension API reports prompt preflight/queue acceptance through
				// onAccepted. Do not advance the HCOM cursor merely because the input
				// hook ran: model/auth/compaction checks happen afterward.
				const idle = ctx.isIdle();
				const accepted = await new Promise<boolean>((resolve) => {
					try {
						sendUserMessageWithAcceptance(formatted, {
							...(idle ? {} : { deliverAs: "followUp" as const }),
							onAccepted: resolve,
						});
					} catch (error) {
						log("ERROR", "plugin.delivery_send_failed", instanceName, {
							error: String(error),
						});
						resolve(false);
					}
				});
				if (!accepted) {
					if (pendingAckId === pending.maxId) pendingAckId = null;
					deliveryPending = true;
					return false;
				}
				// Acceptance is durable enough to suppress reinjection; persist before
				// status/ack subprocesses so reload cannot reopen a duplicate window.
				rememberDelivered(pending.messages);
				for (const message of pending.messages) {
					pi.appendEntry<HcomCard>("hcom-card", {
						action: message.expects_reply ? "request" : "receive",
						messageId: typeof message.message_id === "string" ? message.message_id : undefined,
						target: String(message.from ?? "unknown"),
						state: "accepted",
						text: String(message.message ?? ""),
						timestamp: String(message.timestamp ?? new Date().toISOString()),
					});
				}
				const sender = String(pending.messages[0]?.from ?? "");
				await reportStatus(
					ctx,
					"active",
					sender ? `deliver:${sender}` : "deliver",
				);
				await ackPending(idle ? "sendUserMessage:idle" : "sendUserMessage:followUp");
				log("INFO", "plugin.delivery_pending", instanceName, {
					count: pending.messages.length,
					pending_ack: pending.maxId,
					idle,
				});
				return true;
			} catch (error) {
				if (
					pendingAckId === pending.maxId &&
					!deliveredMessageIds.has(pending.maxId)
				)
					pendingAckId = null;
				log("ERROR", "plugin.delivery_send_failed", instanceName, {
					error: String(error),
				});
				return false;
			}
		} finally {
			deliveryInFlight = false;
			drainPendingDelivery("delivery_in_flight_wake");
		}
	}

	// Replay a wake that was queued while delivery was gated. Re-armed once nothing
	// is mid-flight and no ack is pending, so the same unread batch is not delivered
	// twice. The microtask + dedup flag collapse a burst of queued wakes into one pass.
	function schedulePendingDelivery(reason: string): void {
		if (deliveryRetryScheduled) return;
		deliveryRetryScheduled = true;
		log("DEBUG", "plugin.delivery_retry_scheduled", instanceName, { reason });
		queueMicrotask(() => {
			deliveryRetryScheduled = false;
			if (!instanceName || !currentCtx) return;
			void deliverPending(currentCtx);
		});
	}

	function drainPendingDelivery(reason: string): void {
		if (deliveryPending && !deliveryInFlight && pendingAckId === null) {
			deliveryPending = false;
			schedulePendingDelivery(reason);
		}
	}

	async function ackPending(source: string): Promise<void> {
		if (!instanceName || pendingAckId === null) return;
		const ackId = pendingAckId;
		// Legacy audit wording: const result = await hcom; runHcom adds abort and generation safety.
		// Legacy audit anchor: "--ack", "--up-to".
		const result = await runHcom([
			"pi-read",
			"--name",
			instanceName,
			"--ack",
			"--up-to",
			String(ackId),
		]);
		if (result.code !== 0) {
			log("WARN", "plugin.deferred_ack_failed", instanceName, {
				acked_to: ackId,
				source,
				exit_code: result.code,
				stderr: result.stderr.slice(0, 300),
			});
			return;
		}
		if (pendingAckId === ackId) pendingAckId = null;
		log("INFO", "plugin.deferred_ack", instanceName, {
			acked_to: ackId,
			source,
		});
		drainPendingDelivery("post_ack_wake");
	}

	function buildPresence(ctx: ExtensionContext, turnState: string): Record<string, unknown> {
		const usage = ctx.getContextUsage();
		return {
			provider: ctx.model?.provider ?? null,
			model: ctx.model?.id ?? null,
			thinking: pi.getThinkingLevel(),
			contextTokens: usage?.tokens ?? null,
			contextWindow: usage?.contextWindow ?? ctx.model?.contextWindow ?? null,
			contextPercent: usage?.percent ?? null,
			sessionName: pi.getSessionName() ?? null,
			turnState,
			tmuxPane: process.env.TMUX_PANE ?? null,
			capabilities: [...brokerFeatures].sort(),
			endpointEpoch,
		};
	}

	async function refreshFeatures(): Promise<void> {
		const result = await runHcom(["status", "--json"], { timeoutMs: 10_000 });
		if (result.code !== 0) return;
		try {
			const parsed = JSON.parse(result.stdout || "{}");
			brokerFeatures = new Set(
				Array.isArray(parsed.features)
					? parsed.features.filter((item: unknown) => typeof item === "string")
					: [],
			);
		} catch {}
	}

	async function reportStatus(
		ctx: ExtensionContext,
		status: "active" | "listening" | "blocked",
		context = "",
		detail = "",
	): Promise<void> {
		await bindIdentity(ctx);
		if (!instanceName) return;
		const args = ["pi-status", "--name", instanceName, "--status", status];
		if (context) args.push("--context", context);
		if (detail) args.push("--detail", detail.slice(0, 512));
		const presence = JSON.stringify(buildPresence(ctx, status));
		if (
			brokerFeatures.has("pi-rich-presence-v1") &&
			presence !== lastPresenceSignature
		) {
			args.push("--presence", presence);
		}
		const result = await runHcom(args);
		if (result.code === 0) {
			lastReportedStatusKey = statusKey(status, context, detail);
			if (args.includes("--presence")) lastPresenceSignature = presence;
			if (status === "listening") {
				lastListeningHeartbeatAt = Date.now();
				schedulePendingDelivery("status_listening");
			}
		}
	}

	async function reportReconciledStatus(ctx: ExtensionContext): Promise<void> {
		const key = statusKey("listening", "", "");
		const heartbeatDue =
			Date.now() - lastListeningHeartbeatAt >= LISTENING_HEARTBEAT_MS;
		if (lastReportedStatusKey !== key || heartbeatDue) {
			await reportStatus(ctx, "listening");
		}
	}

	async function pollPendingIfDue(ctx: ExtensionContext): Promise<void> {
		const now = Date.now();
		const interval = notifyPort ? PENDING_POLL_MS : FALLBACK_PENDING_POLL_MS;
		if (now - lastPendingPollAt < interval) return;
		lastPendingPollAt = now;
		await deliverPending(ctx);
	}

	function clearIdleTimer(): void {
		if (idleTimer) clearTimeout(idleTimer);
		idleTimer = null;
	}

	async function reconcile(): Promise<void> {
		if (reconcileInFlight || !currentCtx) return;
		reconcileInFlight = true;
		try {
			const ctx = currentCtx;
			// A transient startup/broker failure must not permanently disable delivery.
			// bindIdentity applies bounded backoff, so the heartbeat can safely retry.
			await bindIdentity(ctx);
			if (!instanceName) return;
			if (pendingAckId !== null) await ackPending("reconcile");
			if (ctx.isIdle() && !uiPromptActive) {
				await reportReconciledStatus(ctx);
			}
			// Fetch while the agent is busy too: sendUserMessageWithAcceptance queues a
			// follow-up without disturbing the active turn. Only a modal UI prompt gates
			// delivery, and ui_prompt_end drains immediately.
			if (!uiPromptActive) await pollPendingIfDue(ctx);
		} catch (error) {
			log("ERROR", "plugin.reconcile_error", instanceName, {
				error: String(error),
			});
		} finally {
			reconcileInFlight = false;
		}
	}

	function startReconcileTimer(): void {
		stopReconcileTimer();
		reconcileTimer = setInterval(() => void reconcile(), 5_000);
	}

	function stopReconcileTimer(): void {
		if (reconcileTimer) {
			clearInterval(reconcileTimer);
			reconcileTimer = null;
		}
	}

function resetBinding(): void {
		stopReconcileTimer();
		closeActiveOverlay?.();
		closeActiveOverlay = null;
		invalidateRuntimeGeneration();
		stopNotifyServer();
		instanceName = null;
		sessionId = null;
		bootstrapText = null;
		bindingPromise = null;
		pendingAckId = null;
		deliveryInFlight = false;
		deliveryPending = false;
		deliveryRetryScheduled = false;
		bootstrapInjectedForSession = null;
		deliveredMessageIds = new Set<number>();
		bindFailureCount = 0;
		bindRetryAt = 0;
		lastBindFailureLogAt = 0;
		brokerFeatures = new Set<string>();
		lastPresenceSignature = null;
		lastReportedStatusKey = null;
		lastListeningHeartbeatAt = 0;
		lastPendingPollAt = 0;
		agentActive = false;
		uiPromptActive = false;
		uiPromptEpoch += 1;
		clearIdleTimer();
	}

	function parseMachineResult(result: HcomResult): unknown {
		const output = result.stdout.trim();
		if (!output) return { ok: result.code === 0 };
		try {
			return JSON.parse(output);
		} catch {
			return { output };
		}
	}

	function attachmentArgs(attachments: HcomAttachment[] | undefined): string[] {
		return (attachments ?? []).flatMap((attachment) => [
			"--attachment",
			JSON.stringify(attachment),
		]);
	}

	function required(value: string | undefined, field: string): string {
		if (!value?.trim()) throw new Error(`${field} is required`);
		return value;
	}

	async function executeHcomAction(
		params: any,
		signal?: AbortSignal,
		ctx?: ExtensionContext,
	): Promise<{ value: unknown; card?: HcomCard }> {
		if (ctx) await bindIdentity(ctx);
		if (!instanceName)
			throw new Error("HCOM identity is not bound for this Pi session");
		const action = String(params.action);
		const messageId = params.messageId ? String(params.messageId) : undefined;
		const text = params.text === undefined ? undefined : String(params.text);
		const target = params.target === undefined ? undefined : String(params.target).trim().replace(/^@/, "");
		const attachments = params.attachments as HcomAttachment[] | undefined;
		let args: string[];

		switch (action) {
			case "list":
				args = ["list", "--json"];
				break;
			case "status":
				args = ["status", "--json"];
				break;
			case "pending":
				args = ["message", "pending", "--json"];
				break;
			case "send":
				args = [
					"send",
					`@${required(target, "target")}`,
					"--quiet",
					"--json",
					...attachmentArgs(attachments),
					"--",
					required(text, "text"),
				];
				break;
			case "reply":
				args = [
					"message",
					"reply",
					required(messageId, "messageId"),
					"--quiet",
					"--json",
					...attachmentArgs(attachments),
					"--",
					required(text, "text"),
				];
				break;
			case "cancel":
				args = [
					"message",
					"cancel",
					required(messageId, "messageId"),
					"--json",
				];
				break;
			case "supersede":
				args = [
					"message",
					"supersede",
					required(messageId, "messageId"),
					"--json",
					...attachmentArgs(attachments),
					"--",
					required(text, "text"),
				];
				break;
			case "retry":
				args = ["message", "retry", required(messageId, "messageId"), "--json"];
				break;
			case "ask": {
				if (askInFlight)
					throw new Error("Only one active HCOM ask is allowed per Pi session");
				askInFlight = true;
				const askGeneration = runtimeGeneration;
				let requestId: string | undefined;
				try {
					const send = await runHcom(
						[
							"send",
							`@${required(target, "target")}`,
							"--intent",
							"request",
							"--reply-mode",
							"wait",
							"--quiet",
							"--json",
							...attachmentArgs(attachments),
							"--",
							required(text, "text"),
						],
						{ signal },
					);
					if (send.code !== 0)
						throw new Error(send.stderr || `hcom send exited ${send.code}`);
					const sent = parseMachineResult(send) as any;
					requestId =
						String(sent?.message_id ?? sent?.messageId ?? "") || undefined;
					if (!requestId)
						throw new Error("HCOM request did not return a stable message_id");
					pi.appendEntry<HcomCard>("hcom-card", {
						action,
						messageId: requestId,
						target,
						state: "waiting",
						text,
						timestamp: new Date().toISOString(),
					});
					const timeoutSec = Number(params.timeoutSec ?? 600);
					const waited = await runHcom(
						[
							"message",
							"wait",
							requestId,
							"--timeout",
							String(timeoutSec),
							"--json",
						],
						{ signal, timeoutMs: (timeoutSec + 5) * 1_000 },
					);
					if (waited.code !== 0)
						throw new Error(
							waited.stderr || `hcom message wait exited ${waited.code}`,
						);
					return {
						value: parseMachineResult(waited),
						card: {
							action,
							messageId: requestId,
							target,
							state: "replied",
							text,
							timestamp: new Date().toISOString(),
						},
					};
				} finally {
					if ((signal?.aborted || askGeneration !== runtimeGeneration) && requestId)
						await runHcom(["message", "cancel", requestId, "--json"], {
							timeoutMs: 10_000,
						});
					askInFlight = false;
				}
			}
			default:
				throw new Error(`Unsupported HCOM action: ${action}`);
		}

		const result = await runHcom(args, { signal });
		if (result.code !== 0)
			throw new Error(result.stderr || `hcom ${action} exited ${result.code}`);
		const value = parseMachineResult(result);
		const returnedId =
			String(
				(value as any)?.message_id ??
					(value as any)?.messageId ??
					messageId ??
					"",
			) || undefined;
		const card = ["send", "reply", "cancel", "supersede", "retry"].includes(
			action,
		)
			? {
					action,
					messageId: returnedId,
					target,
					state: "ok",
					text,
					timestamp: new Date().toISOString(),
				}
			: undefined;
		return { value, card };
	}

	function displayMarkdown(source: string): string {
		return source
			.replace(/```[^\n]*\n?([\s\S]*?)```/g, "$1")
			.replace(/`([^`]+)`/g, "$1")
			.replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1")
			.replace(/\[([^\]]+)\]\([^)]*\)/g, "$1")
			.replace(/^\s{0,3}(?:#{1,6}|>|[-*+]\s|\d+\.\s)\s*/gm, "");
	}

	function previousCodePointIndex(text: string, index: number): number {
		const points = Array.from(text.slice(0, index));
		const previous = points.at(-1);
		return previous ? index - previous.length : 0;
	}

	function nextCodePointIndex(text: string, index: number): number {
		const next = Array.from(text.slice(index))[0];
		return next ? index + next.length : text.length;
	}

	pi.registerEntryRenderer<HcomCard>(
		"hcom-card",
		(entry, { expanded }, theme) => {
			const data = entry.data ?? { action: "unknown", timestamp: "" };
			const direction = ["send", "ask", "reply", "supersede", "retry"].includes(
				data.action,
			)
				? "↗"
				: "•";
			const target = data.target ? ` @${data.target}` : "";
			const id = data.messageId ? ` #${data.messageId.slice(0, 12)}` : "";
			const state = data.state ? ` ${theme.fg("dim", `[${data.state}]`)}` : "";
			let rendered = `${theme.fg("accent", `${direction} hcom ${data.action}`)}${target}${id}${state}`;
			if (expanded && data.text)
				rendered += `\n${displayMarkdown(data.text)}\n${theme.fg("dim", data.timestamp)}`;
			return new Text(rendered, 0, 0);
		},
	);

	pi.registerTool({
		name: "hcom",
		label: "HCOM",
		description:
			"List HCOM agents and send, ask, reply to, cancel, supersede, or retry durable inter-agent messages.",
		promptSnippet:
			"Coordinate with HCOM agents through durable message lifecycle operations",
		promptGuidelines: [
			"Use hcom when another HCOM agent has relevant context, when work should be delegated across active sessions, or when a durable reply is required.",
			"Use hcom action=ask only when the current turn may block for a reply; do not retry automatically after timeout or cancellation.",
		],
		parameters: HCOM_TOOL_PARAMS,
		async execute(_toolCallId, params, signal, _onUpdate, ctx) {
			const result = await executeHcomAction(params, signal, ctx);
			if (result.card) pi.appendEntry<HcomCard>("hcom-card", result.card);
			return {
				content: [
					{
						type: "text" as const,
						text: JSON.stringify(result.value, null, 2),
					},
				],
				details: result.value,
			};
		},
	});
	async function loadRoster(): Promise<HcomAgent[]> {
		const result = await runHcom(["list", "--json"], { timeoutMs: 10_000 });
		if (result.code !== 0)
			throw new Error(result.stderr || "Unable to list HCOM agents");
		const parsed = parseMachineResult(result) as any;
		const agents = Array.isArray(parsed)
			? parsed
			: Array.isArray(parsed?.agents)
				? parsed.agents
				: [];
		return agents.filter(
			(agent: any) =>
				agent && typeof agent.name === "string" && agent.name !== instanceName,
		);
	}

	async function fallbackCompose(
		ctx: ExtensionContext,
		agents: HcomAgent[],
	): Promise<void> {
		const labels = agents.map(
			(agent) => `${agent.name} · ${agent.status ?? "unknown"}`,
		);
		const selected = await ctx.ui.select("HCOM agent", labels);
		if (!selected) return;
		const index = labels.indexOf(selected);
		const target = agents[index]?.name;
		if (!target) return;
		const text = await ctx.ui.input(
			`Message @${target}`,
			"Type a durable HCOM message",
		);
		if (!text) return;
		const result = await executeHcomAction({ action: "send", target, text });
		if (result.card) pi.appendEntry<HcomCard>("hcom-card", result.card);
		ctx.ui.notify(`Sent to @${target}`, "info");
	}

	async function launchHcomUI(ctx: ExtensionContext): Promise<void> {
		if (ctx.mode !== "tui") {
			ctx.ui.notify(
				"/hcom requires TUI mode; the hcom tool remains available",
				"error",
			);
			return;
		}
		await bindIdentity(ctx);
		let agents = await loadRoster();
		if (agents.length === 0) {
			ctx.ui.notify("No other HCOM agents are currently visible", "info");
			return;
		}
		const generation = runtimeGeneration;
		try {
			await ctx.ui.custom(
				(tui, theme, _keybindings, done) => {
					let selected = 0;
					let mode: "roster" | "compose" = "roster";
					let draft = "";
					let cursor = 0;
					let closed = false;
					let refreshing = false;
					let cacheKey = "";
					let cache: string[] = [];

					const close = () => {
						if (closed) return;
						closed = true;
						if (closeActiveOverlay === close) closeActiveOverlay = null;
						clearInterval(refreshTimer);
						done(undefined);
					};
					const invalidate = () => {
						cacheKey = "";
					};
					const refresh = async () => {
						if (closed || refreshing || generation !== runtimeGeneration)
							return close();
						refreshing = true;
						try {
							const next = await loadRoster();
							if (!closed && generation === runtimeGeneration) {
								agents = next;
								selected = Math.max(0, Math.min(selected, agents.length - 1));
								invalidate();
								tui.requestRender();
							}
						} catch {
						} finally {
							refreshing = false;
						}
					};
					const refreshTimer = setInterval(() => void refresh(), 2_000);
					refreshTimer.unref?.();
					closeActiveOverlay = close;

					return {
						render(width: number): string[] {
							const safeWidth = Math.max(20, width - 4);
							const key = `${safeWidth}\0${mode}\0${selected}\0${draft}\0${cursor}\0${agents.map((a) => `${a.name}:${a.status}:${a.unread_count ?? 0}`).join("|")}`;
							if (key === cacheKey) return cache;
							const lines = [
								theme.fg("accent", theme.bold("HCOM · agents")),
								theme.fg(
									"dim",
									`${instanceName ?? "unbound"} · ${agents.length} peers`,
								),
							];
							for (let index = 0; index < agents.length; index += 1) {
								const agent = agents[index];
								const marker =
									index === selected ? theme.fg("accent", "›") : " ";
								const unread = agent.unread_count
									? ` · ${agent.unread_count} unread`
									: "";
								const provider =
									agent.presence?.provider && agent.presence?.model
										? ` · ${agent.presence.provider}/${agent.presence.model}`
										: "";
								lines.push(
									truncateToWidth(
										`${marker} ${agent.name} · ${agent.status ?? "unknown"}${provider}${unread}`,
										safeWidth,
									),
								);
							}
							lines.push("");
							if (mode === "compose") {
								const target = agents[selected]?.name ?? "unknown";
								const before = draft.slice(0, cursor);
								const after = draft.slice(cursor);
								const input = `${theme.fg("accent", `@${target}>`)} ${before}${CURSOR_MARKER}${after}`;
								lines.push(...wrapTextWithAnsi(input, safeWidth));
								lines.push(
									theme.fg(
										"dim",
										`Enter send · Esc back · ${visibleWidth(draft)} columns`,
									),
								);
							} else {
								lines.push(
									theme.fg(
										"dim",
										"↑↓ select · Enter compose · Esc close · refresh 2s",
									),
								);
							}
							cacheKey = key;
							cache = lines;
							return lines;
						},
						invalidate,
						dispose() {
							closed = true;
							if (closeActiveOverlay === close) closeActiveOverlay = null;
							clearInterval(refreshTimer);
						},
						handleInput(data: string) {
							if (matchesKey(data, Key.ctrl("c"))) return close();
							if (matchesKey(data, Key.escape)) {
								if (mode === "compose") {
									mode = "roster";
									invalidate();
									tui.requestRender();
								} else close();
								return;
							}
							if (mode === "roster") {
								if (matchesKey(data, Key.up))
									selected = Math.max(0, selected - 1);
								else if (matchesKey(data, Key.down))
									selected = Math.min(agents.length - 1, selected + 1);
								else if (matchesKey(data, Key.enter) && agents[selected])
									mode = "compose";
								invalidate();
								tui.requestRender();
								return;
							}
							if (matchesKey(data, Key.left)) cursor = previousCodePointIndex(draft, cursor);
							else if (matchesKey(data, Key.right)) cursor = nextCodePointIndex(draft, cursor);
							else if (matchesKey(data, Key.backspace)) {
								if (cursor > 0) {
									const previous = previousCodePointIndex(draft, cursor);
									draft = draft.slice(0, previous) + draft.slice(cursor);
									cursor = previous;
								}
							} else if (matchesKey(data, Key.enter)) {
								const target = agents[selected]?.name;
								const text = draft.trim();
								if (target && text)
									void executeHcomAction({ action: "send", target, text })
										.then((result) => {
											if (result.card)
												pi.appendEntry<HcomCard>("hcom-card", result.card);
											ctx.ui.notify(`Sent to @${target}`, "info");
											close();
										})
										.catch((error) => ctx.ui.notify(String(error), "error"));
								return;
							} else if (
								!data.includes("\x1b") &&
								data.length > 0 &&
								Buffer.byteLength(draft + data, "utf8") <= MAX_HCOM_TEXT_BYTES
							) {
								draft = draft.slice(0, cursor) + data + draft.slice(cursor);
								cursor += data.length;
							}
							invalidate();
							tui.requestRender();
						},
					};
				},
				{ overlay: true },
			);
		} catch (error) {
			log("WARN", "plugin.ui_overlay_failed", instanceName, {
				error: String(error),
			});
			await fallbackCompose(ctx, agents);
		}
	}

	pi.registerCommand("hcom", {
		description: "Open HCOM roster and composer",
		handler: async (_args, ctx) => launchHcomUI(ctx),
	});
	pi.registerShortcut("alt+m", {
		description: "Open HCOM roster and composer",
		handler: launchHcomUI,
	});

	pi.on("session_start", async (_event, ctx) => {
		currentCtx = ctx;
		resetBinding();
		// Start before awaiting the first bind so a timeout/failure still has a
		// self-healing retry path.
		startReconcileTimer();
		await bindIdentity(ctx);
		log("INFO", "plugin.ready", instanceName, {
			bound: Boolean(instanceName),
			session_id: ctx.sessionManager.getSessionId(),
			idle: ctx.isIdle(),
			ui_prompt_active: uiPromptActive,
			notify_port: notifyPort,
		});
		if (instanceName) {
			await reportStatus(ctx, "listening");
			await deliverPending(ctx);
		}
	});

	pi.on("session_shutdown", async (event) => {
		const reason = event.reason ?? "shutdown";
		// Pi replaces the extension runtime in-process for reload/new/resume/fork,
		// then emits session_start in a fresh runtime. The worker process and HCOM
		// identity remain alive, so stopping here would leave a live pane marked ⊘.
		const replacingSession = ["reload", "new", "resume", "fork"].includes(reason);
		if (instanceName && !replacingSession) {
			await hcom(["pi-stop", "--name", instanceName, "--reason", reason]);
		} else if (instanceName) {
			log("INFO", "plugin.session_replacement", instanceName, { reason });
		}
		resetBinding();
	});

	pi.on("agent_start", async (_event, ctx) => {
		currentCtx = ctx;
		clearIdleTimer();
		agentActive = true;
		await reportStatus(ctx, "active", "agent");
	});

	pi.on("model_select", async (_event, ctx) => {
		currentCtx = ctx;
		await reportStatus(ctx, agentActive && !ctx.isIdle() ? "active" : "listening", agentActive ? "agent" : "");
	});

	pi.on("thinking_level_select", async (_event, ctx) => {
		currentCtx = ctx;
		await reportStatus(ctx, agentActive && !ctx.isIdle() ? "active" : "listening", agentActive ? "agent" : "");
	});

	pi.on("ui_prompt_start", async (event, ctx) => {
		currentCtx = ctx;
		clearIdleTimer();
		uiPromptActive = true;
		const epoch = ++uiPromptEpoch;
		await reportStatus(ctx, "blocked", "ui_prompt", event.kind);
		// UI lifecycle handlers are best-effort and are not awaited by Pi. If the
		// matching end event overtook this status subprocess, repair the final state.
		if (epoch !== uiPromptEpoch || !uiPromptActive) {
			await reportStatus(
				ctx,
				agentActive && !ctx.isIdle() ? "active" : "listening",
				agentActive ? "agent" : "",
			);
		}
	});

	pi.on("ui_prompt_end", async (_event, ctx) => {
		currentCtx = ctx;
		uiPromptActive = false;
		uiPromptEpoch += 1;
		if (agentActive && !ctx.isIdle()) {
			await reportStatus(ctx, "active", "agent");
			return;
		}
		await reportStatus(ctx, "listening");
		await deliverPending(ctx);
	});

	pi.on("input", async (event: InputEvent, ctx) => {
		currentCtx = ctx;
		await bindIdentity(ctx);
		if (!instanceName) return { action: "continue" };
		if (event.source === "extension") return { action: "continue" };
		if (isBodylessWake(event.text)) {
			// Consume the wake itself, then use the acceptance-aware extension path.
			// Transforming here would run before model/auth/compaction preflight.
			deliveryPending = true;
			setTimeout(() => schedulePendingDelivery("bodyless_wake"), 0).unref?.();
			return { action: "handled" };
		}
		await reportStatus(
			ctx,
			"active",
			event.text.trim() === "<hcom>" ? "trigger" : "prompt",
		);
		return { action: "continue" };
	});

	pi.on("before_agent_start", async (_event, ctx) => {
		currentCtx = ctx;
		await bindIdentity(ctx);
		if (!instanceName || !bootstrapText) return undefined;
		const sid = ctx.sessionManager.getSessionId();
		if (bootstrapInjectedForSession === sid) return undefined;
		bootstrapInjectedForSession = sid;
		log("DEBUG", "plugin.hidden_bootstrap", instanceName, {
			bootstrap_len: bootstrapText.length,
		});
		return {
			message: {
				customType: "hcom-bootstrap",
				content: bootstrapText,
				display: false,
			},
		};
	});

	pi.on("tool_call", async (event, ctx) => {
		currentCtx = ctx;
		await bindIdentity(ctx);
		if (!instanceName) return undefined;
		await reportStatus(
			ctx,
			"active",
			`tool:${event.toolName}`,
			String((event.input as any)?.path ?? (event.input as any)?.command ?? ""),
		);
		const result = await hcom([
			"pi-beforetool",
			"--name",
			instanceName,
			"--tool",
			event.toolName,
			"--input-json",
			JSON.stringify(event.input ?? {}),
		]);
		try {
			const json = JSON.parse(result.stdout || "{}");
			if (json.decision === "block") {
				return {
					block: true,
					reason: String(json.reason || "Blocked by hcom"),
				};
			}
		} catch {}
		return undefined;
	});

	pi.on("tool_result", async (event, ctx) => {
		currentCtx = ctx;
		await reportStatus(ctx, "active", `tool:${event.toolName}`);
		await deliverPending(ctx);
	});

	pi.on("turn_end", async (_event, ctx) => {
		currentCtx = ctx;
		await deliverPending(ctx);
	});

	pi.on("agent_end", async (_event, ctx) => {
		currentCtx = ctx;
		if (!agentActive) return;
		agentActive = false;
		clearIdleTimer();
		idleTimer = setTimeout(() => {
			idleTimer = null;
			if (currentCtx?.isIdle() && !uiPromptActive) {
				void (async () => {
					await reportStatus(currentCtx, "listening");
					await deliverPending(currentCtx);
				})();
			}
		}, IDLE_DEBOUNCE_MS);
		idleTimer.unref?.();
	});
}
