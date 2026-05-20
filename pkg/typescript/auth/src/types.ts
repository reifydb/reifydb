// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import type { LoginChallengeResult } from "@reifydb/client";

export type AuthStatus =
  | "disconnected"
  | "verifying"
  | "signing"
  | "authenticated"
  | "error";

export interface AuthSession {
  readonly token: string;
  readonly identity: string;
  readonly wallet_address: string;
  readonly expires_at: number;
}

export interface AuthState {
  readonly status: AuthStatus;
  readonly session: AuthSession | null;
  readonly clientReady: boolean;
  readonly error: string | null;
}

export interface WalletConnector {
  readonly connected: boolean;
  readonly connecting: boolean;
  readonly publicKey: string | null;
  readonly hasSelectedWallet: boolean;
  signMessage(message: Uint8Array): Promise<Uint8Array>;
  encodeSignature(bytes: Uint8Array): string;
}

export interface AuthCapableClient {
  login_challenge(
    method: string,
    credentials: Record<string, string>,
  ): Promise<LoginChallengeResult>;
  logout(): Promise<void>;
}

export type { LoginChallengeResult };
