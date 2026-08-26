// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ADDON_PATH = path.resolve(__dirname, '../../../dst-bridge/index.node')
const require = createRequire(import.meta.url)

export interface Backend {
  command(rql: string): string
  query(rql: string): string
}

export interface BackendCtor {
  new (seed: number): Backend
}

let ctor: BackendCtor | null = null

// loads once per test file; the addon boots uptime's real, unmodified schema, never a mocked copy
export function loadBackend(): BackendCtor {
  if (ctor != null) return ctor
  try {
    ;({ DstEngine: ctor } = require(ADDON_PATH))
  } catch (err) {
    throw new Error(
      `dst-bridge addon not built at ${ADDON_PATH}. From the repo root, run:\n` +
        `  REIFYDB_DST=1 cargo build --release -p reifydb-uptime-dst-bridge\n` +
        `  cp target/release/libreifydb_uptime_dst_bridge.so pkg/projects/uptime/dst-bridge/index.node\n` +
        `Original error: ${err}`,
    )
  }
  return ctor!
}
