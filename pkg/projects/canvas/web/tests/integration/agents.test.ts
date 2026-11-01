// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { storeClient } from '@reifydb/reifydb'
import { loadAgents } from '@/agents'
import { commitPixels } from '@/commit'
import { ANONYMOUS, loadBackend, type TestDb, type TestFactory } from '../support/backend'
import { markReady, seedTile } from '../support/seed'

let create: TestFactory
let db: TestDb

beforeAll(() => {
  create = loadBackend()
})

beforeEach(async () => {
  db = create()
  await seedTile(db, 0, 0)
  await seedTile(db, 1, 0)
  await markReady(db)
})

async function userClient(name: string) {
  const response = JSON.parse(await db.authenticate('password', { identifier: name, password: 'x' }))
  if (response.status !== 'authenticated') throw new Error(`login failed: ${JSON.stringify(response)}`)
  return storeClient(db, { identity: response.identity })
}

describe('loadAgents', () => {
  it('lists named painters by pixels painted, readable by anonymous', async () => {
    // This is the whole agents page in one call; the real query must run for a watcher who never logged in.
    const agent = await userClient('agent-a')
    const other = await userClient('agent-b')
    expect(await commitPixels(agent, [{ x: 1, y: 1, color: 3 }])).toEqual({ ok: true, committed: 1 })
    expect(
      await commitPixels(other, [
        { x: 2, y: 1, color: 3 },
        { x: 65, y: 1, color: 4 },
        { x: 3, y: 1, color: 5 },
      ]),
    ).toEqual({ ok: true, committed: 3 })

    const agents = await loadAgents(storeClient(db, { identity: ANONYMOUS }))

    expect(agents.painters.map((p) => [p.name, p.painted])).toEqual([
      ['agent-b', 3],
      ['agent-a', 1],
    ])
    expect(agents.painters.every((p) => p.last > 0)).toBe(true)
  })
})
