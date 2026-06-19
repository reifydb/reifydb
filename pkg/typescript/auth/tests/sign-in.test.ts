// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it, vi } from "vitest";

import { performSignIn } from "../src/sign-in";
import type {
  AuthCapableClient,
  LoginChallengeResult,
  WalletConnector,
} from "../src/types";
import type { AuthTransport } from "../src/transport";

function make_stub_client(
  challenge: LoginChallengeResult,
  authed: LoginChallengeResult,
): AuthCapableClient & { login_challenge: ReturnType<typeof vi.fn> } {
  const fn = vi
    .fn<AuthCapableClient["login_challenge"]>()
    .mockResolvedValueOnce(challenge)
    .mockResolvedValueOnce(authed);
  return {
    login_challenge: fn,
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

function make_wallet(
  publicKey: string | null,
  encodeSignature = (bytes: Uint8Array): string => `enc:${bytes.length}`,
): Pick<WalletConnector, "publicKey" | "signMessage" | "encodeSignature"> {
  return {
    publicKey,
    signMessage: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
    encodeSignature,
  };
}

describe.each([
  ["ws", "ws" as const],
  ["http", "http" as const],
])("performSignIn (%s)", (_label, kind) => {
  it("walks challenge -> sign -> submit and returns a session", async () => {
    const client = make_stub_client(
      { kind: "challenge", challenge_id: "c1", message: "hello", nonce: "n1" },
      { kind: "authenticated", token: "tok", identity: "id" },
    );
    const transport = make_transport(client, kind);
    const wallet = make_wallet("WaLLeT");

    const session = await performSignIn({
      url: "ws://test",
      transport,
      method: "solana",
      wallet,
      domain: "example.com",
      statement: "Sign in",
      sessionTtlSeconds: 60,
    });

    expect(session.token).toBe("tok");
    expect(session.identity).toBe("id");
    expect(session.wallet_address).toBe("WaLLeT");
    expect(session.expires_at).toBeGreaterThan(Math.floor(Date.now() / 1000));

    // Two login_challenge calls in order: first the request, then the response.
    expect(client.login_challenge).toHaveBeenCalledTimes(2);
    const [first_call, second_call] = client.login_challenge.mock.calls;
    expect(first_call[0]).toBe("solana");
    expect(first_call[1]).toMatchObject({
      identifier: "WaLLeT",
      public_key: "WaLLeT",
      domain: "example.com",
      statement: "Sign in",
    });
    expect(second_call[1]).toMatchObject({
      challenge_id: "c1",
      signature: "enc:3",
      signed_message: "hello",
    });

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("releases the transient client even when the second call fails", async () => {
    const fn = vi
      .fn<AuthCapableClient["login_challenge"]>()
      .mockResolvedValueOnce({
        kind: "challenge",
        challenge_id: "c1",
        message: "hello",
        nonce: "n1",
      })
      .mockRejectedValueOnce(new Error("boom"));
    const client: AuthCapableClient = {
      login_challenge: fn,
      logout: vi.fn().mockResolvedValue(undefined),
    };
    const transport = make_transport(client, kind);
    const wallet = make_wallet("W");

    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet,
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/boom/);

    expect(transport.release).toHaveBeenCalledWith(client);
  });

  it("rejects when first response is not a challenge", async () => {
    const client = make_stub_client(
      { kind: "authenticated", token: "t", identity: "i" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = make_transport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: make_wallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/expected challenge/);
  });

  it("rejects when second response is not authenticated", async () => {
    const client = make_stub_client(
      { kind: "challenge", challenge_id: "c", message: "m", nonce: "n" },
      { kind: "challenge", challenge_id: "c2", message: "m2", nonce: "n2" },
    );
    const transport = make_transport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: make_wallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/expected authenticated/);
  });

  it("rejects when publicKey is null", async () => {
    const client = make_stub_client(
      { kind: "challenge", challenge_id: "c", message: "m", nonce: "n" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = make_transport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: make_wallet(null),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 60,
      }),
    ).rejects.toThrow(/publicKey is required/);
  });

  it("rejects when sessionTtlSeconds is not positive", async () => {
    const client = make_stub_client(
      { kind: "challenge", challenge_id: "c", message: "m", nonce: "n" },
      { kind: "authenticated", token: "t", identity: "i" },
    );
    const transport = make_transport(client, kind);
    await expect(
      performSignIn({
        url: "u",
        transport,
        method: "solana",
        wallet: make_wallet("W"),
        domain: "d",
        statement: "s",
        sessionTtlSeconds: 0,
      }),
    ).rejects.toThrow(/sessionTtlSeconds/);
  });
});
