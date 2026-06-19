// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, describe, expect, it } from "vitest";
import { Keypair } from "@solana/web3.js";
import nacl from "tweetnacl";
import bs58 from "bs58";
import {
  performSignIn,
  ws_transport,
  type WalletConnector,
} from "@reifydb/auth";

import { WS_URL, wait_for_database } from "./setup";
import { make_test_wallet } from "./test-wallet";

function sign_in_args(wallet: WalletConnector) {
  return {
    url: WS_URL,
    transport: ws_transport,
    method: "solana",
    wallet,
    domain: "test",
    statement: "Sign in to ReifyDB",
    sessionTtlSeconds: 3600,
  };
}

describe("performSignIn — error paths against real server", () => {
  beforeAll(async () => {
    await wait_for_database();
  }, 30000);

  it("happy-path round-trip succeeds (harness sanity)", async () => {
    const { wallet, publicKeyB58 } = make_test_wallet();
    const session = await performSignIn(sign_in_args(wallet));
    expect(session.wallet_address).toBe(publicKeyB58);
    expect(session.token.length).toBeGreaterThan(0);
  });

  it("server rejects when wallet signs a different message than the challenge", async () => {
    const keypair = Keypair.generate();
    const wrong_msg_wallet: WalletConnector = {
      connected: true,
      connecting: false,
      publicKey: keypair.publicKey.toBase58(),
      hasSelectedWallet: true,
      async signMessage(_message: Uint8Array): Promise<Uint8Array> {
        // Sign a fixed unrelated payload, ignoring the challenge's message.
        // signed_message in the second login_challenge call will be the real
        // challenge.message, so ed25519 verify(real_msg, sig_over_fake) fails.
        return nacl.sign.detached(
          new Uint8Array([0, 1, 2, 3, 4, 5]),
          keypair.secretKey,
        );
      },
      encodeSignature(bytes: Uint8Array): string {
        return bs58.encode(bytes);
      },
    };

    await expect(
      performSignIn(sign_in_args(wrong_msg_wallet)),
    ).rejects.toThrow(/AUTH_FAILED/);
  });

  it("server rejects when signature is produced by a different keypair", async () => {
    const victim = Keypair.generate();
    const attacker = Keypair.generate();

    const mismatched: WalletConnector = {
      connected: true,
      connecting: false,
      publicKey: victim.publicKey.toBase58(),
      hasSelectedWallet: true,
      async signMessage(message: Uint8Array): Promise<Uint8Array> {
        // Server auto-provisions with victim's pubkey on step 1; this signs
        // with attacker's secret, so ed25519 verify against victim's pubkey
        // must fail.
        return nacl.sign.detached(message, attacker.secretKey);
      },
      encodeSignature(bytes: Uint8Array): string {
        return bs58.encode(bytes);
      },
    };

    await expect(performSignIn(sign_in_args(mismatched))).rejects.toThrow(
      /AUTH_FAILED/,
    );
  });

  it("fails fast (no network) when publicKey is empty string", async () => {
    const { wallet } = make_test_wallet();
    const empty: WalletConnector = { ...wallet, publicKey: "" };
    await expect(performSignIn(sign_in_args(empty))).rejects.toThrow(
      /wallet\.publicKey is required/,
    );
  });

  it("fails fast (no network) when publicKey is null", async () => {
    const { wallet } = make_test_wallet();
    const nullpk: WalletConnector = { ...wallet, publicKey: null };
    await expect(performSignIn(sign_in_args(nullpk))).rejects.toThrow(
      /wallet\.publicKey is required/,
    );
  });

  it("fails fast (no network) when sessionTtlSeconds is non-positive", async () => {
    const { wallet } = make_test_wallet();
    await expect(
      performSignIn({ ...sign_in_args(wallet), sessionTtlSeconds: 0 }),
    ).rejects.toThrow(/sessionTtlSeconds must be a positive number/);
  });
});
