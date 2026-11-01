// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadTestFactory, storeClient, type BridgeClient, type TestDb, type TestFactory } from '@reifydb/reifydb'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ADDON_PATH = path.resolve(__dirname, '../../../../../../target/gate/libcanvas_bridge.so')

export const ANONYMOUS = '00000000-0000-7000-8000-000000000000'

export type { BridgeClient, TestDb, TestFactory }

export function loadBackend(): TestFactory {
  try {
    return loadTestFactory(ADDON_PATH)
  } catch (err) {
    throw new Error(
      `bridge addon not built at ${ADDON_PATH}. From the repo root, run:\n` +
        `  cargo build --profile gate -p reifydb-canvas-bridge\n` +
        `Original error: ${err}`,
    )
  }
}

export async function userClient(db: TestDb, identifier = 'painter'): Promise<BridgeClient> {
  const response = JSON.parse(await db.authenticate('password', { identifier, password: 'x' }))
  if (response.status !== 'authenticated') throw new Error(`password login failed: ${JSON.stringify(response)}`)
  return storeClient(db, { identity: response.identity })
}
