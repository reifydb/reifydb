// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Reifydb } from '../src/index'
import { Shape, Int4Value, Utf8Value } from '@reifydb/core'

describe('migration', () => {
  it('rejects when the migrations directory does not exist', () => {
    // must surface as a catchable error, not crash the process or hang the promise
    expect(() => Reifydb.memory().withMigrations({ dir: '/no/such/directory' }).build()).toThrow()
  })

  it('applies inline rql statements passed directly, without touching the filesystem', async () => {
    const db = Reifydb.memory()
      .withMigrations({
        name: 'inline',
        statements: ['create namespace inline_smoke', 'create table inline_smoke::items { id: int4, label: utf8 }'],
      })
      .build()

    await db.commandRoot('insert inline_smoke::items [{ id: 1, label: "hello" }]', {}, [])
    const [rows] = await db.queryRoot(
      'from inline_smoke::items map { id, label }',
      {},
      [Shape.object({ id: Shape.int4Value(), label: Shape.utf8Value() })],
    )

    expect(rows).toEqual([{ id: new Int4Value(1), label: new Utf8Value('hello') }])
  })

  it('builds with no migrations at all', async () => {
    const db = Reifydb.memory().build()

    // must reject, not silently return empty, since the namespace itself was never created
    await expect(db.queryRoot('from smoke::items map { id, label }', {}, [])).rejects.toThrow()
  })
})
