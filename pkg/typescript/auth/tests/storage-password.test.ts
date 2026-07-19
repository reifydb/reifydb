// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeEach, describe, expect, it } from "vitest";

import {
  readStoredSession,
  storageKeyFor,
  writeStoredSession,
} from "../src/storage";
import type { AuthSession } from "../src/types";

const NS = "test.storage.password";

beforeEach(() => {
  localStorage.clear();
});

describe("storage (password sessions)", () => {
  it("round-trips a password session with method and identifier intact", () => {
    const session: AuthSession = {
      token: "tok",
      identity: "id",
      wallet_address: "user@example.com",
      identifier: "user@example.com",
      method: "password",
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    writeStoredSession(NS, session);
    const read = readStoredSession(NS);
    expect(read).toEqual(session);
    expect(read?.method).toBe("password");
    expect(read?.identifier).toBe("user@example.com");
  });

  it("still accepts a legacy four-field wallet session", () => {
    // Sessions persisted before the password flow existed have no method or
    // identifier; the upgrade must not log those users out.
    const legacy = {
      token: "tok",
      identity: "id",
      wallet_address: "WalletA",
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    localStorage.setItem(storageKeyFor(NS), JSON.stringify(legacy));
    expect(readStoredSession(NS)).toEqual(legacy);
  });

  it("rejects a session with an unknown method value", () => {
    const bad = {
      token: "tok",
      identity: "id",
      wallet_address: "user@example.com",
      method: "magic-link",
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    localStorage.setItem(storageKeyFor(NS), JSON.stringify(bad));
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(storageKeyFor(NS))).toBeNull();
  });

  it("rejects a session with a non-string identifier", () => {
    const bad = {
      token: "tok",
      identity: "id",
      wallet_address: "user@example.com",
      identifier: 42,
      method: "password",
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    localStorage.setItem(storageKeyFor(NS), JSON.stringify(bad));
    expect(readStoredSession(NS)).toBeNull();
    expect(localStorage.getItem(storageKeyFor(NS))).toBeNull();
  });
});
