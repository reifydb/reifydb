// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it, vi } from "vitest";

import { performPasswordSignIn } from "../src/sign-in-password";
import type {
  AuthCapableClient,
  CredentialAuthCapableClient,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

function make_password_client(): CredentialAuthCapableClient & {
  login_with_password: ReturnType<typeof vi.fn>;
} {
  return {
    login_challenge: vi.fn(),
    login_with_password: vi
      .fn<CredentialAuthCapableClient["login_with_password"]>()
      .mockResolvedValue({ token: "tok", identity: "id" }),
    logout: vi.fn().mockResolvedValue(undefined),
  };
}

function make_transport(
  client: AuthCapableClient,
  kind: "ws" | "http",
): AuthTransport & { release: ReturnType<typeof vi.fn> } {
  return {
    kind,
    connect: vi.fn().mockResolvedValue(client),
    release: vi.fn(),
  };
}

describe.each([
  ["ws", "ws" as const],
  ["http", "http" as const],
])("performPasswordSignIn (%s)", (_label, kind) => {
  it("logs in with identifier + password and returns a password session", async () => {
    const client = make_password_client();
    const transport = make_transport(client, kind);

    const session = await performPasswordSignIn({
      url: "u",
      transport,
      identifier: "user@example.com",
      password: "hunter2",
      sessionTtlSeconds: 60,
    });

    // The session must carry the identifier as the principal binding so the
    // provider's cross-tab principal comparison keeps working for password
    // sessions, and method must mark it as a password session.
    expect(session.token).toBe("tok");
    expect(session.identity).toBe("id");
    expect(session.wallet_address).toBe("user@example.com");
    expect(session.identifier).toBe("user@example.com");
    expect(session.method).toBe("password");
    expect(session.expires_at).toBeGreaterThan(Math.floor(Date.now() / 1000));

    expect(client.login_with_password).toHaveBeenCalledExactlyOnceWith(
      "user@example.com",
      "hunter2",
    );
    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("propagates wrong-credential failures and still releases the client", async () => {
    const client = make_password_client();
    client.login_with_password.mockRejectedValueOnce(
      new Error("invalid credentials"),
    );
    const transport = make_transport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "wrong",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/invalid credentials/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects clients without login_with_password and still releases", async () => {
    // A minimal AuthCapableClient (challenge-only) must fail loudly instead of
    // silently degrading - password sign-in has no challenge fallback.
    const client: AuthCapableClient = {
      login_challenge: vi.fn(),
      logout: vi.fn().mockResolvedValue(undefined),
    };
    const transport = make_transport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "hunter2",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/does not support login_with_password/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects an empty identifier before connecting", async () => {
    const client = make_password_client();
    const transport = make_transport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "",
        password: "hunter2",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/identifier is required/);

    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("rejects an empty password before connecting", async () => {
    const client = make_password_client();
    const transport = make_transport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/password is required/);

    expect(transport.connect).not.toHaveBeenCalled();
  });

  it("rejects when sessionTtlSeconds is not positive", async () => {
    const client = make_password_client();
    const transport = make_transport(client, kind);

    await expect(
      performPasswordSignIn({
        url: "u",
        transport,
        identifier: "user@example.com",
        password: "hunter2",
        sessionTtlSeconds: 0,
      }),
    ).rejects.toThrow(/sessionTtlSeconds/);
  });
});
