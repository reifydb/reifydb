// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Shape, ShapeMismatch } from '@reifydb/core'
import { Store, rql } from '@reifydb/store'
import { Reifydb } from '../src/reifydb'
import { storeClient } from '../src/store-client'

const itemShape = Shape.object({ id: Shape.int4(), name: Shape.utf8() })
const totalShape = Shape.object({ total: Shape.int4() })

function build() {
  return Reifydb.memory()
    .withMigrations({
      name: 'init',
      statements: [
        'create namespace shop',
        'create table shop::items { id: int4, name: utf8 }',
        'create table shop::totals { total: int4 }',
      ],
    })
    .build()
}

describe('spec frames over the native bridge', () => {
  it('pairs each output frame with the shape at the same position', async () => {
    // Swapped frames would decode item rows as totals, so pairing must follow statement order.
    const client = storeClient(build())
    await client.command(
      "insert shop::items [{ id: 1, name: 'a' }, { id: 2, name: 'b' }]; insert shop::totals [{ total: 2 }]",
      null,
      [],
    )
    const store = new Store(client)
    const spec = rql([itemShape, totalShape])`output from shop::items sort { id: asc }; output from shop::totals`
    const [items, totals] = await store.query(spec, null)
    expect(items.map((row) => row.id)).toEqual([1, 2])
    expect(totals.map((row) => row.total)).toEqual([2])
    expect(store.getEntry(spec, null).data).toEqual([
      [
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
      ],
      [{ total: 2 }],
    ])
  })

  it('rejects with a shape mismatch when the frame count differs from the shape count and marks the entry error', async () => {
    // Without the count check a missing frame would read as an empty result instead of a wrong spec.
    const store = new Store(storeClient(build()))
    const spec = rql([itemShape, totalShape])`from shop::items`
    await expect(store.query(spec, null)).rejects.toBeInstanceOf(ShapeMismatch)
    const entry = store.getEntry(spec, null)
    expect(entry.status).toBe('error')
    expect(entry.error).toBeInstanceOf(ShapeMismatch)
  })
})
