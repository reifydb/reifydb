// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Int4Value, ListValue, Shape } from '@reifydb/core'
import { Reifydb } from '../src/reifydb'
import { storeClient } from '../src/store-client'

const rowShape = Shape.object({ id: Shape.int4(), position: Shape.int4() })

function build(body: string) {
  return Reifydb.memory()
    .withMigrations({
      name: 'init',
      statements: [
        'create namespace app',
        'create table app::t { id: int4, position: int4 }',
        `create procedure app::add_all { ids: list(int4) } as { ${body} }`,
      ],
    })
    .build()
}

function ids(...values: number[]): ListValue {
  return new ListValue(values.map(value => new Int4Value(value)))
}

describe('procedure list parameter over the native bridge', () => {
  it('takes a list parameter and loops over it with for', async () => {
    // Without list parameters a write of unknown arity needs one generated statement per row.
    const client = storeClient(build('for $id in $ids { insert app::t [{ id: $id, position: 0 }] }'))
    await client.command('call app::add_all($ids)', { ids: ids(10, 20, 30) }, [])
    const [rows] = await client.query('from app::t map { id, position }', null, [rowShape])
    expect(rows.map(row => row.id).sort((a, b) => a - b)).toEqual([10, 20, 30])
  })

  it('keeps a counter across iterations so each row gets its list position', async () => {
    // Status page monitors are ordered by position, so a loop without an index loses the order the caller chose.
    const client = storeClient(
      build('let $position = 0; for $id in $ids { insert app::t [{ id: $id, position: $position }]; $position = $position + 1 }'),
    )
    await client.command('call app::add_all($ids)', { ids: ids(30, 10, 20) }, [])
    const [rows] = await client.query('from app::t map { id, position }', null, [rowShape])
    expect([...rows].sort((a, b) => a.position - b.position)).toEqual([
      { id: 30, position: 0 },
      { id: 10, position: 1 },
      { id: 20, position: 2 },
    ])
  })
})
