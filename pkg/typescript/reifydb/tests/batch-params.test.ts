// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Shape } from '@reifydb/core'
import type { SubscriptionRow } from '@reifydb/client'
import { Reifydb } from '../src/reifydb'
import { storeClient } from '../src/store-client'

const rowShape = Shape.object({ id: Shape.int4(), owner: Shape.string() })
const rql = 'from app::v filter { owner == $owner } map { id, owner }'

function build() {
  return Reifydb.memory()
    .withMigrations({
      name: 'init',
      statements: [
        'create namespace app',
        'create table app::t { id: int4, owner: utf8 }',
        'create deferred view app::v { id: int4, owner: utf8 } as { from app::t map { id, owner } }',
      ],
    })
    .build()
}

describe('batch subscription params over the native bridge', () => {
  it('filters each member with its own params, on hydration and on later changes', async () => {
    // Each member must bind its own params, otherwise every member of a per-monitor batch sees every monitor's rows.
    const client = storeClient(build())
    await client.command(`insert app::t [{ id: 1, owner: 'a' }]`, null, [])
    await client.caughtUp()

    const first: SubscriptionRow<any>[] = []
    const second: SubscriptionRow<any>[] = []
    await client.batchSubscribe([
      { rql, params: { owner: 'a' }, shape: rowShape, callbacks: { onInsert: (rows) => first.push(...rows) } },
      { rql, params: { owner: 'b' }, shape: rowShape, callbacks: { onInsert: (rows) => second.push(...rows) } },
    ])
    await client.command(`insert app::t [{ id: 2, owner: 'b' }]`, null, [])
    await client.caughtUp()

    expect(first).toEqual([expect.objectContaining({ id: 1, owner: 'a' })])
    expect(second).toEqual([expect.objectContaining({ id: 2, owner: 'b' })])
  })

  it('refuses a params list that does not pair with the queries', async () => {
    // A short list must be refused, otherwise the unpaired queries silently run without params.
    const db = build()

    await expect(db.batchSubscribeRoot(['from app::v', 'from app::v'], [{ owner: 'a' }])).rejects.toThrow(
      /one params entry per query/,
    )
  })
})
