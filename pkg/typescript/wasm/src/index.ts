// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { decode, type TypeValuePair } from '@reifydb/core';
import type { WasmDB as RawWasmDB } from '../wasm/reifydb_webassembly';

export { decode } from '@reifydb/core';
export type { TypeValuePair, Value } from '@reifydb/core';

export interface LoginResult {
  token: string;
  identity: string;
}

/**
 * Recursively transforms raw WASM output into decoded Value instances.
 *
 * The WASM engine returns typed values as `{type, value}` pairs.
 * This function walks the result tree and decodes each pair using `@reifydb/core`'s `decode()`.
 */
export function transformToValueInstances(result: unknown): unknown {
  if (result === null || result === undefined) return result;
  if (typeof result !== 'object') return result;
  if (Array.isArray(result)) {
    return result.map(transformToValueInstances);
  }
  const obj = result as Record<string, unknown>;
  if ('type' in obj && 'value' in obj && Object.keys(obj).length === 2) {
    return decode(obj as unknown as TypeValuePair);
  }
  const transformed: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    transformed[key] = transformToValueInstances(value);
  }
  return transformed;
}

/**
 * High-level wrapper around the raw WASM database engine.
 *
 * Provides typed methods for admin, command, and query operations,
 * automatically decoding WASM type-value pairs into Value instances.
 */
export class WasmDB {
  private db: RawWasmDB;

  constructor(db: RawWasmDB) {
    this.db = db;
  }

  admin(rql: string): unknown {
    return transformToValueInstances(this.db.admin(rql));
  }

  adminWithParams(rql: string, params: unknown): unknown {
    return transformToValueInstances(this.db.adminWithParams(rql, params));
  }

  command(rql: string): unknown {
    return transformToValueInstances(this.db.command(rql));
  }

  commandWithParams(rql: string, params: unknown): unknown {
    return transformToValueInstances(this.db.commandWithParams(rql, params));
  }

  query(rql: string): unknown {
    return transformToValueInstances(this.db.query(rql));
  }

  queryWithParams(rql: string, params: unknown): unknown {
    return transformToValueInstances(this.db.queryWithParams(rql, params));
  }

  loginWithPassword(identifier: string, password: string): LoginResult {
    const raw = this.db.loginWithPassword(identifier, password);
    const result = { token: raw.token, identity: raw.identity };
    raw.free();
    return result;
  }

  loginWithToken(token: string): LoginResult {
    const raw = this.db.loginWithToken(token);
    const result = { token: raw.token, identity: raw.identity };
    raw.free();
    return result;
  }

  logout(): void {
    this.db.logout();
  }

  free(): void {
    this.db.free();
  }
}

/**
 * Creates a new WasmDB instance by dynamically importing the WASM module.
 *
 * The dynamic import ensures the WASM glue code is not bundled by tsup
 * and is instead resolved by the consumer's bundler (Vite, webpack, etc.).
 *
 * @example
 * ```typescript
 * import { createWasmDB } from '@reifydb/wasm';
 *
 * const db = await createWasmDB();
 * db.admin('CREATE NAMESPACE demo');
 * const results = db.query('FROM demo.users');
 * db.free();
 * ```
 */
export async function createWasmDB(): Promise<WasmDB> {
  const mod = await import('../wasm/reifydb_webassembly.js');
  return new WasmDB(new mod.WasmDB());
}
