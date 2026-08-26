// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ADDON_PATH = path.resolve(__dirname, '../../../../../../target/release/libbridge.so')

export interface Backend {
  commandRoot(rql: string): Promise<string>
  queryRoot(rql: string): Promise<string>
  commandAs(identity: string, rql: string): Promise<string>
  queryAs(identity: string, rql: string): Promise<string>
  authenticate(method: string, credentials: Record<string, string>): Promise<string>
}

export interface BackendFactory {
  (seed: number): Backend
}

let factory: BackendFactory | null = null

// dlopen, not require: .so has no require() extension handler, so it must be loaded directly
export function loadBackend(): BackendFactory {
  if (factory != null) return factory
  try {
    const addon = { exports: {} as { create: BackendFactory } }
    process.dlopen(addon as unknown as NodeJS.Module, ADDON_PATH)
    factory = addon.exports.create
  } catch (err) {
    throw new Error(
      `bridge addon not built at ${ADDON_PATH}. From the repo root, run:\n` +
        `  REIFYDB_DST=1 cargo build --release -p reifydb-uptime-bridge\n` +
        `Original error: ${err}`,
    )
  }
  return factory!
}
