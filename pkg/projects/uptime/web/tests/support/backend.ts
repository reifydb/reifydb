// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ADDON_PATH = path.resolve(__dirname, '../../../../../../target/release/libbridge.so')

export interface Backend {
  command(rql: string): string
  query(rql: string): string
}

export interface BackendCtor {
  new (seed: number): Backend
}

let ctor: BackendCtor | null = null

// dlopen, not require: .so has no require() extension handler, so it must be loaded directly
export function loadBackend(): BackendCtor {
  if (ctor != null) return ctor
  try {
    const addon = { exports: {} as { DstEngine: BackendCtor } }
    process.dlopen(addon as unknown as NodeJS.Module, ADDON_PATH)
    ctor = addon.exports.DstEngine
  } catch (err) {
    throw new Error(
      `bridge addon not built at ${ADDON_PATH}. From the repo root, run:\n` +
        `  REIFYDB_DST=1 cargo build --release -p reifydb-uptime-bridge\n` +
        `Original error: ${err}`,
    )
  }
  return ctor!
}
