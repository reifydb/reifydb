// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useMemo } from "react";
import { useWallet } from "@solana/wallet-adapter-react";
import type { WalletConnector } from "@reifydb/auth";

import { encode_base58 } from "./base58";

// Solana wallet adapter stores the selected wallet name in this localStorage
// key; presence implies an autoConnect attempt is pending after a refresh.
const SOLANA_WALLET_NAME_KEY = "walletName";

function read_has_selected_wallet(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return localStorage.getItem(SOLANA_WALLET_NAME_KEY) != null;
  } catch {
    return false;
  }
}

export function useSolanaWalletConnector(): WalletConnector {
  const { connected, connecting, publicKey, signMessage } = useWallet();

  return useMemo<WalletConnector>(() => {
    const pk = publicKey != null ? publicKey.toBase58() : null;
    return {
      connected,
      connecting,
      publicKey: pk,
      hasSelectedWallet: read_has_selected_wallet(),
      async signMessage(message: Uint8Array): Promise<Uint8Array> {
        if (signMessage == null) {
          throw new Error(
            "@reifydb/auth-solana: connected wallet does not support signMessage",
          );
        }
        return signMessage(message);
      },
      encodeSignature(bytes: Uint8Array): string {
        return encode_base58(bytes);
      },
    };
  }, [connected, connecting, publicKey, signMessage]);
}
