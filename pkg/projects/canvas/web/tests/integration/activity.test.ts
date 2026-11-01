// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, describe, expect, it } from 'vitest'
import { Activity, EDIT_RQL } from '@/activity'
import { bytes, numeric, text } from '@/client'
import { commitPixels } from '@/commit'
import { loadBackend, userClient, type TestFactory } from '../support/backend'
import { markReady, seedTile } from '../support/seed'

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('activity from the live edit stream', () => {
  it('logs each committed tile under the painter name', async () => {
    // The painter watches itself because the test bridge streams live rows only to the committing client.
    const db = create()
    await seedTile(db, 0, 0)
    await seedTile(db, 1, 0)
    await markReady(db)
    const alice = await userClient(db, 'alice')
    const activity = new Activity(() => 7)
    await alice.subscribe([
      {
        rql: EDIT_RQL,
        params: {},
        config: { hydration: { enabled: false } },
        callbacks: {
          onInsert: (rows: Record<string, unknown>[]) => {
            for (const row of rows) activity.put(text(row.name), bytes(row.pixels), numeric(row.painted))
          },
        },
      },
    ])

    expect(
      await commitPixels(alice, [
        { x: 1, y: 1, color: 3 },
        { x: 65, y: 1, color: 4 },
        { x: 66, y: 1, color: 4 },
      ]),
    ).toEqual({ ok: true, committed: 3 })
    await alice.caughtUp()

    const logged = activity
      .latest()
      .map(({ name, tx, ty, px, color }) => ({ name, tx, ty, px, color }))
      .sort((a, b) => a.tx - b.tx)
    expect(logged).toEqual([
      { name: 'alice', tx: 0, ty: 0, px: 1, color: 3 },
      { name: 'alice', tx: 1, ty: 0, px: 2, color: 4 },
    ])
  })
})
