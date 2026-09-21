// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Int4Value, ListValue, RecordValue, Shape, Utf8Value } from '@reifydb/core'
import { Reifydb } from '../src/reifydb'
import { storeClient } from '../src/store-client'

const orderShape = Shape.object({ id: Shape.int4(), customer_id: Shape.int4() })
const lineShape = Shape.object({ order_id: Shape.int4(), sku: Shape.utf8(), qty: Shape.int4() })

function build() {
  return Reifydb.memory()
    .withMigrations({
      name: 'init',
      statements: [
        'create namespace shop',
        'create table shop::orders { id: int4, customer_id: int4 }',
        'create table shop::order_lines { order_id: int4, sku: utf8, qty: int4 }',
      ],
    })
    .build()
}

function line(sku: string, qty: number): RecordValue {
  return new RecordValue({ order_id: new Int4Value(1), sku: new Utf8Value(sku), qty: new Int4Value(qty) })
}

describe('insert returning over the native bridge', () => {
  it('returns the inserted rows instead of the count frame', async () => {
    // Without returned rows an output insert cannot hand the created row back to the caller.
    const client = storeClient(build())
    const [orders] = await client.command(
      'insert shop::orders [{ id: 1, customer_id: 7 }] returning { id, customer_id }',
      null,
      [orderShape],
    )
    expect(orders).toEqual([{ '#rownum': 1, id: 1, customerId: 7 }])
  })

  it('returns one row per record of a list parameter', async () => {
    // A list insert that drops returning would leave a multi-row output write with no rows to return.
    const client = storeClient(build())
    const [lines] = await client.command(
      'insert shop::order_lines $lines returning { order_id, sku, qty }',
      { lines: new ListValue([line('apple', 2), line('pear', 5)]) },
      [lineShape],
    )
    expect(lines).toEqual([
      { '#rownum': 1, orderId: 1, sku: 'apple', qty: 2 },
      { '#rownum': 2, orderId: 1, sku: 'pear', qty: 5 },
    ])
  })
})
