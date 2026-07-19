// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { LoginChallengeResult, LoginResult } from "@reifydb/client";

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
  readonly method?: "wallet" | "password";
  readonly identifier?: string;
}

export interface PasswordCredentials {
  readonly identifier: string;
  readonly password: string;
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

export interface CredentialAuthCapableClient extends AuthCapableClient {
  login_with_password(
    identifier: string,
    password: string,
  ): Promise<LoginResult>;
}

export type { LoginChallengeResult, LoginResult };
