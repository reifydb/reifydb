// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import React, {
  createContext,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
} from "react";

import { clearClient, currentClient, ensureClient } from "./client-cache";
import { performSignIn } from "./sign-in";
import {
  clearStoredSession,
  readStoredSession,
  storageKeyFor,
  writeStoredSession,
} from "./storage";
import type {
  AuthCapableClient,
  AuthSession,
  AuthState,
  WalletConnector,
} from "./types";
import type { AuthTransport } from "./transport";

const DEFAULT_TTL_SECONDS = 86400;

export interface AuthContextValue extends AuthState {
  signIn(): Promise<void>;
  signOut(): Promise<void>;
}

export const AuthContext = createContext<AuthContextValue | null>(null);

export interface AuthProviderProps<
  TClient extends AuthCapableClient = AuthCapableClient,
> {
  url: string;
  transport: AuthTransport<TClient>;
  storageNamespace: string;
  method: string;
  domain: string;
  statement: string;
  sessionTtlSeconds?: number;
  wallet: WalletConnector;
  children: ReactNode;
}

interface InternalState {
  status: AuthState["status"];
  session: AuthSession | null;
  clientReady: boolean;
  error: string | null;
}

export function AuthProvider<TClient extends AuthCapableClient>(
  props: AuthProviderProps<TClient>,
): ReactElement {
  const {
    url,
    transport,
    storageNamespace,
    method,
    domain,
    statement,
    sessionTtlSeconds = DEFAULT_TTL_SECONDS,
    wallet,
    children,
  } = props;

  const [state, setState] = useState<InternalState>(() => {
    const stored = readStoredSession(storageNamespace);
    return stored
      ? { status: "verifying", session: stored, clientReady: false, error: null }
      : { status: "disconnected", session: null, clientReady: false, error: null };
  });

  // Pull live wallet fields into stable primitives for effect deps.
  const wallet_connected = wallet.connected;
  const wallet_connecting = wallet.connecting;
  const wallet_public_key = wallet.publicKey;
  const wallet_has_selected = wallet.hasSelectedWallet;

  // Refs so callbacks can read latest values without re-binding.
  const session_ref = useRef(state.session);
  session_ref.current = state.session;
  const wallet_ref = useRef(wallet);
  wallet_ref.current = wallet;

  const tear_down = useCallback(
    (next_status: InternalState["status"], next_error: string | null) => {
      clearStoredSession(storageNamespace);
      clearClient();
      setState({
        status: next_status,
        session: null,
        clientReady: false,
        error: next_error,
      });
    },
    [storageNamespace],
  );

  // Wallet-match gate: the security invariant. The authenticated client is only
  // ever constructed when the connected wallet matches the persisted session.
  useEffect(() => {
    const session = state.session;
    if (session == null) return;

    if (wallet_connected && wallet_public_key != null) {
      if (wallet_public_key !== session.wallet_address) {
        tear_down("disconnected", null);
        return;
      }
      let cancelled = false;
      ensureClient(transport, url, session.token)
        .then(() => {
          if (cancelled) return;
          setState((prev) =>
            prev.session === session
              ? { ...prev, status: "authenticated", clientReady: true, error: null }
              : prev,
          );
        })
        .catch((err: unknown) => {
          if (cancelled) return;
          const message = err instanceof Error ? err.message : "Failed to connect client";
          tear_down("error", message);
        });
      return () => {
        cancelled = true;
      };
    }

    if (!wallet_connected && (wallet_connecting || wallet_has_selected)) {
      // Wallet adapter is autoConnecting; stay in verifying. Do NOT construct
      // the client yet - we must not authenticate before the match is confirmed.
      setState((prev) =>
        prev.session === session
          ? { ...prev, status: "verifying", clientReady: false }
          : prev,
      );
      return;
    }

    // Wallet is definitively absent (no selected wallet, not connecting). User
    // explicitly disconnected, so the session can no longer be proven; tear down.
    tear_down("disconnected", null);
  }, [
    state.session,
    wallet_connected,
    wallet_connecting,
    wallet_public_key,
    wallet_has_selected,
    transport,
    url,
    tear_down,
  ]);

  // Cross-tab defense: if our storage entry is changed or cleared by another
  // tab, re-validate and tear down if it no longer matches our in-memory state.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const key = storageKeyFor(storageNamespace);
    const on_storage = (e: StorageEvent) => {
      if (e.key !== key) return;
      const current = session_ref.current;
      if (e.newValue == null) {
        if (current != null) tear_down("disconnected", null);
        return;
      }
      try {
        const parsed = JSON.parse(e.newValue) as Partial<AuthSession>;
        if (
          current == null ||
          parsed.token !== current.token ||
          parsed.wallet_address !== current.wallet_address ||
          parsed.identity !== current.identity
        ) {
          tear_down("disconnected", null);
        }
      } catch {
        tear_down("disconnected", null);
      }
    };
    window.addEventListener("storage", on_storage);
    return () => {
      window.removeEventListener("storage", on_storage);
    };
  }, [storageNamespace, tear_down]);

  const signIn = useCallback(async () => {
    const w = wallet_ref.current;
    if (!w.connected || w.publicKey == null) {
      setState((prev) => ({ ...prev, status: "error", error: "Wallet not connected" }));
      return;
    }
    setState((prev) => ({ ...prev, status: "signing", error: null }));
    try {
      const session = await performSignIn({
        url,
        transport,
        method,
        wallet: w,
        domain,
        statement,
        sessionTtlSeconds,
      });
      // Sanity: signed-in wallet must match the live publicKey. If the user
      // swapped wallets while the signature was in flight, refuse the session.
      if (session.wallet_address !== wallet_ref.current.publicKey) {
        throw new Error("Wallet changed during sign in");
      }
      writeStoredSession(storageNamespace, session);
      setState({
        status: "verifying",
        session,
        clientReady: false,
        error: null,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Sign in failed";
      setState({
        status: "error",
        session: null,
        clientReady: false,
        error: message,
      });
    }
  }, [
    url,
    transport,
    method,
    domain,
    statement,
    sessionTtlSeconds,
    storageNamespace,
  ]);

  const signOut = useCallback(async () => {
    // Best-effort server-side logout via the currently-cached client.
    try {
      await currentClient().logout();
    } catch {
      // no current client, or server-side logout failed; tear down regardless
    }
    tear_down("disconnected", null);
  }, [tear_down]);

  const value = useMemo<AuthContextValue>(
    () => ({
      status: state.status,
      session: state.session,
      clientReady: state.clientReady,
      error: state.error,
      signIn,
      signOut,
    }),
    [state, signIn, signOut],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
