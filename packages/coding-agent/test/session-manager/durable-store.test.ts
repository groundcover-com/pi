import { describe, expect, it } from "vitest";
import {
	SessionManager,
	type SessionPersistenceAppendInput,
	type SessionPersistenceRewriteInput,
	type SessionPersistenceSnapshot,
	type SessionPersistenceStore,
} from "../../src/core/session-manager.ts";

class MemorySessionStore implements SessionPersistenceStore {
	public snapshot: SessionPersistenceSnapshot | null = null;
	public appends: SessionPersistenceAppendInput[] = [];
	public rewrites: SessionPersistenceRewriteInput[] = [];
	public failNextAppend = false;

	async load(): Promise<SessionPersistenceSnapshot | null> {
		return this.snapshot;
	}

	async initialize(input: SessionPersistenceRewriteInput): Promise<void> {
		this.rewrites.push(input);
		this.snapshot = {
			header: input.header,
			entries: input.entries,
			leafId: input.leafId,
			sessionFile: input.sessionFile,
			flushed: input.flushed,
		};
	}

	async appendEntry(input: SessionPersistenceAppendInput): Promise<void> {
		if (this.failNextAppend) {
			this.failNextAppend = false;
			throw new Error("durable append failed");
		}
		this.appends.push(input);
		if (!this.snapshot) {
			this.snapshot = { header: input.header, entries: [], leafId: input.previousLeafId };
		}
		this.snapshot = {
			...this.snapshot,
			header: input.header,
			entries: [...this.snapshot.entries, input.entry],
			leafId: input.nextLeafId,
			sessionFile: input.sessionFile,
		};
	}

	async rewrite(input: SessionPersistenceRewriteInput): Promise<void> {
		this.rewrites.push(input);
		this.snapshot = {
			header: input.header,
			entries: input.entries,
			leafId: input.leafId,
			sessionFile: input.sessionFile,
			flushed: input.flushed,
		};
	}
}

describe("SessionManager durable store", () => {
	it("initializes an empty store and durably appends entries before advancing the leaf", async () => {
		const store = new MemorySessionStore();
		const session = await SessionManager.createWithStore("/tmp/pi-durable", store, {
			newSession: { id: "durable-session" },
		});

		expect(store.rewrites[0]).toMatchObject({ reason: "initialize" });
		expect(session.getSessionId()).toBe("durable-session");
		const firstId = await session.appendMessageAsync({ role: "user", content: "hello", timestamp: 1 });
		const secondId = await session.appendCustomEntryAsync("zubin", { ok: true });

		expect(store.appends).toHaveLength(2);
		expect(store.appends[0]).toMatchObject({ previousLeafId: null, nextLeafId: firstId, entryCount: 1 });
		expect(store.appends[1]).toMatchObject({ previousLeafId: firstId, nextLeafId: secondId, entryCount: 2 });
		expect(store.snapshot?.entries.map((entry) => entry.id)).toEqual([firstId, secondId]);
		expect(session.getLeafId()).toBe(secondId);
	});

	it("loads existing session entries from the store", async () => {
		const store = new MemorySessionStore();
		const original = await SessionManager.createWithStore("/tmp/pi-durable", store, {
			newSession: { id: "durable-session" },
		});
		const messageId = await original.appendMessageAsync({ role: "user", content: "hello", timestamp: 1 });

		const loaded = await SessionManager.createWithStore("/tmp/ignored", store);

		expect(loaded.getSessionId()).toBe("durable-session");
		expect(loaded.getCwd()).toBe("/tmp/pi-durable");
		expect(loaded.getEntries()).toHaveLength(1);
		expect(loaded.getLeafId()).toBe(messageId);
	});

	it("does not advance in-memory state when durable append fails", async () => {
		const store = new MemorySessionStore();
		const session = await SessionManager.createWithStore("/tmp/pi-durable", store, {
			newSession: { id: "durable-session" },
		});
		store.failNextAppend = true;

		await expect(session.appendMessageAsync({ role: "user", content: "hello", timestamp: 1 })).rejects.toThrow(
			"durable append failed",
		);

		expect(session.getEntries()).toHaveLength(0);
		expect(session.getLeafId()).toBeNull();
		expect(store.snapshot?.entries).toHaveLength(0);
	});

	it("keeps branch summary async state atomic when durable append fails", async () => {
		const store = new MemorySessionStore();
		const session = await SessionManager.createWithStore("/tmp/pi-durable", store, {
			newSession: { id: "durable-session" },
		});
		const firstId = await session.appendMessageAsync({ role: "user", content: "hello", timestamp: 1 });
		const secondId = await session.appendCustomEntryAsync("zubin", { ok: true });
		store.failNextAppend = true;

		await expect(session.branchWithSummaryAsync(firstId, "branch summary")).rejects.toThrow("durable append failed");

		expect(session.getLeafId()).toBe(secondId);
		expect(session.getEntries().map((entry) => entry.id)).toEqual([firstId, secondId]);
		expect(store.snapshot?.leafId).toBe(secondId);
		expect(store.snapshot?.entries.map((entry) => entry.id)).toEqual([firstId, secondId]);

		const summaryId = await session.branchWithSummaryAsync(firstId, "branch summary");
		const summaryAppend = store.appends[store.appends.length - 1];
		expect(summaryAppend).toMatchObject({ previousLeafId: secondId, nextLeafId: summaryId });
		expect(summaryAppend?.entry).toMatchObject({ id: summaryId, parentId: firstId, type: "branch_summary" });
		expect(session.getLeafId()).toBe(summaryId);
	});
});
