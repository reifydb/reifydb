// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadTestFactory, type TestDb, type TestFactory } from '@reifydb/reifydb'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ADDON_PATH = path.resolve(__dirname, '../../../../../../target/release/libbridge.so')

export type { TestDb, TestFactory }

export function loadBackend(): TestFactory {
  try {
    return loadTestFactory(ADDON_PATH)
  } catch (err) {
    throw new Error(
      `bridge addon not built at ${ADDON_PATH}. From the repo root, run:\n` +
        `  REIFYDB_DST=1 cargo build --release -p reifydb-uptime-bridge\n` +
        `Original error: ${err}`,
    )
  }
}
