// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  BaseMessageSignerWalletAdapter,
  WalletReadyState,
  type SupportedTransactionVersions,
  type WalletName,
} from "@solana/wallet-adapter-base";
import {
  Keypair,
  PublicKey,
  type Transaction,
  type VersionedTransaction,
} from "@solana/web3.js";
import nacl from "tweetnacl";

export const MOCK_WALLET_NAME = "Mock" as WalletName<"Mock">;

// In-process Solana wallet adapter for integration tests. Wraps a generated
// Keypair and signs messages with tweetnacl. Transaction signing is stubbed
// because the auth flow only needs signMessage.
export class MockSolanaWalletAdapter extends BaseMessageSignerWalletAdapter {
  name = MOCK_WALLET_NAME;
  url = "https://mock.invalid";
  icon = "";
  readyState: WalletReadyState = WalletReadyState.Installed;
  supportedTransactionVersions: SupportedTransactionVersions = null;

  private _keypair: Keypair;
  private _publicKey: PublicKey | null = null;
  private _connecting = false;

  constructor(keypair?: Keypair) {
    super();
    this._keypair = keypair ?? Keypair.generate();
  }

  get publicKey(): PublicKey | null {
    return this._publicKey;
  }

  get connecting(): boolean {
    return this._connecting;
  }

  get keypair(): Keypair {
    return this._keypair;
  }

  async connect(): Promise<void> {
    if (this._publicKey != null || this._connecting) return;
    if (
      this.readyState !== WalletReadyState.Installed &&
      this.readyState !== WalletReadyState.Loadable
    ) {
      throw new Error("Mock wallet not ready");
    }
    this._connecting = true;
    try {
      this._publicKey = this._keypair.publicKey;
      this.emit("connect", this._publicKey);
    } finally {
      this._connecting = false;
    }
  }

  async disconnect(): Promise<void> {
    if (this._publicKey == null) return;
    this._publicKey = null;
    this.emit("disconnect");
  }

  async signMessage(message: Uint8Array): Promise<Uint8Array> {
    if (this._publicKey == null) {
      throw new Error("Mock wallet not connected");
    }
    return nacl.sign.detached(message, this._keypair.secretKey);
  }

  async signTransaction<T extends Transaction | VersionedTransaction>(
    _tx: T,
  ): Promise<T> {
    throw new Error(
      "MockSolanaWalletAdapter.signTransaction is not implemented; the auth flow does not use it",
    );
  }
}
