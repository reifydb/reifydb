// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Keypair } from "@solana/web3.js";
import nacl from "tweetnacl";
import bs58 from "bs58";
import type { WalletConnector } from "@reifydb/auth";

export interface TestWallet {
  wallet: WalletConnector;
  keypair: Keypair;
  publicKeyB58: string;
}

export function make_test_wallet(): TestWallet {
  const keypair = Keypair.generate();
  const publicKeyB58 = keypair.publicKey.toBase58();
  const wallet: WalletConnector = {
    connected: true,
    connecting: false,
    publicKey: publicKeyB58,
    hasSelectedWallet: true,
    async signMessage(message: Uint8Array): Promise<Uint8Array> {
      return nacl.sign.detached(message, keypair.secretKey);
    },
    encodeSignature(bytes: Uint8Array): string {
      return bs58.encode(bytes);
    },
  };
  return { wallet, keypair, publicKeyB58 };
}
