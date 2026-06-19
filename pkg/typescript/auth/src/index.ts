// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export type {
  AuthCapableClient,
  AuthSession,
  AuthState,
  AuthStatus,
  LoginChallengeResult,
  WalletConnector,
} from "./types";

export type { AuthTransport } from "./transport";
export {
  http_transport,
  json_http_transport,
  json_ws_transport,
  ws_transport,
} from "./transport";

export {
  AuthContext,
  AuthProvider,
  type AuthContextValue,
  type AuthProviderProps,
} from "./auth-provider";

export { useAuth, useAuthClient } from "./use-auth";

export { performSignIn, type SignInArgs } from "./sign-in";

export {
  clearStoredSession,
  readStoredSession,
  storageKeyFor,
  writeStoredSession,
} from "./storage";

export { clearClient, currentClient, ensureClient } from "./client-cache";

export * from "@reifydb/core";
export * from "@reifydb/client";
