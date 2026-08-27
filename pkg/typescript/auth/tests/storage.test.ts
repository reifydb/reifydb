// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeEach, describe, expect, it } from "vitest";

import {
  clearStoredSession,
  readStoredSession,
  storageKeyFor,
  sweepExpiredSessions,
  tabScopedNamespace,
  writeStoredSession,
} from "../src/storage";
import type { AuthSession } from "../src/types";

const NS = "test.ns";
const KEY = storageKeyFor(NS);

function futureSession(over: Partial<AuthSession> = {}): AuthSession {
  return {
    token: "tok_abc",
    identity: "id_123",
    walletAddress: "WaLLeT0000000000000000000000000000000000000",
    expiresAt: Math.floor(Date.now() / 1000) + 3600,
    ...over,
  };
}

beforeEach(() => {
  localStorage.clear();
});

describe("readStoredSession", () => {
  it("returns null when nothing is stored", () => {
    expect(readStoredSession(NS)).toBeNull();
  });

  it("round-trips a valid session", () => {
    const session = futureSession();
    writeStoredSession(NS, session);
    expect(readStoredSession(NS)).toEqual(session);
  });

  it("returns null and wipes storage when JSON is malformed", () => {
    localStorage.setItem(KEY, "not json{");
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("returns null and wipes storage when fields are missing", () => {
    localStorage.setItem(KEY, JSON.stringify({ token: "x", expiresAt: 1 }));
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("returns null and wipes storage when fields have wrong type", () => {
    localStorage.setItem(
      KEY,
      JSON.stringify({
        token: 1,
        identity: "x",
        walletAddress: "w",
        expiresAt: 999999999999,
      }),
    );
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("returns null and wipes storage when expired", () => {
    const past = futureSession({ expiresAt: Math.floor(Date.now() / 1000) - 10 });
    localStorage.setItem(KEY, JSON.stringify(past));
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("rejects empty-string fields (defense in depth)", () => {
    const bad = { ...futureSession(), token: "" };
    localStorage.setItem(KEY, JSON.stringify(bad));
    expect(readStoredSession(NS)).toBeNull();
  });

  it("isolates namespaces", () => {
    writeStoredSession("ns.a", futureSession({ token: "a" }));
    writeStoredSession("ns.b", futureSession({ token: "b" }));
    expect(readStoredSession("ns.a")?.token).toBe("a");
    expect(readStoredSession("ns.b")?.token).toBe("b");
  });
});

describe("writeStoredSession", () => {
  it("refuses to persist a malformed session", () => {
    expect(() =>
      writeStoredSession(NS, { token: "", identity: "", walletAddress: "", expiresAt: 0 } as AuthSession),
    ).toThrow(/malformed session/);
  });
});

describe("clearStoredSession", () => {
  it("removes the namespaced entry", () => {
    writeStoredSession(NS, futureSession());
    clearStoredSession(NS);
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it("is a no-op when nothing is stored", () => {
    expect(() => clearStoredSession(NS)).not.toThrow();
  });
});

describe("tabScopedNamespace", () => {
  it("appends a per-tab segment to the namespace", () => {
    const scoped = tabScopedNamespace(NS);
    expect(scoped.startsWith(`${NS}.`)).toBe(true);
    expect(scoped.length).toBeGreaterThan(NS.length + 1);
  });

  it("is stable across calls within a tab", () => {
    expect(tabScopedNamespace(NS)).toBe(tabScopedNamespace(NS));
  });

  it("shares the tab segment across different base namespaces", () => {
    const segA = tabScopedNamespace("ns.a").slice("ns.a.".length);
    const segB = tabScopedNamespace("ns.b").slice("ns.b.".length);
    expect(segA).toBe(segB);
  });
});

describe("sweepExpiredSessions", () => {
  const expired = () =>
    futureSession({ expiresAt: Math.floor(Date.now() / 1000) - 10 });

  it("removes an expired slot left behind by another tab", () => {
    const orphan = `${NS}.deadtab.auth`;
    localStorage.setItem(orphan, JSON.stringify(expired()));
    sweepExpiredSessions(NS);
    expect(localStorage.getItem(orphan)).toBeNull();
  });

  it("keeps a non-expired slot belonging to another live tab", () => {
    const live = `${NS}.livetab.auth`;
    const value = JSON.stringify(futureSession());
    localStorage.setItem(live, value);
    sweepExpiredSessions(NS);
    expect(localStorage.getItem(live)).toBe(value);
  });

  it("never removes the current tab's own slot", () => {
    // Even an expired entry at our own key is left for readStoredSession to handle.
    const own = storageKeyFor(tabScopedNamespace(NS));
    const value = JSON.stringify(expired());
    localStorage.setItem(own, value);
    sweepExpiredSessions(NS);
    expect(localStorage.getItem(own)).toBe(value);
  });

  it("ignores expired slots under a different base namespace", () => {
    const other = "other.ns.deadtab.auth";
    const value = JSON.stringify(expired());
    localStorage.setItem(other, value);
    sweepExpiredSessions(NS);
    expect(localStorage.getItem(other)).toBe(value);
  });

  it("is a no-op when storage is empty", () => {
    expect(() => sweepExpiredSessions(NS)).not.toThrow();
  });
});
