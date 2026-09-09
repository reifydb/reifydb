// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Reifydb } from '../src/index'
import { Option, Shape } from '@reifydb/core'
import type { Db } from '../src/db'

const ADD = 'CALL notes::add($id, $label)'
const STORED = 'from notes::rows filter { id == $id } map { label }'
const LABEL = Shape.object({ label: Shape.option(Shape.string()) })

async function setup(): Promise<Db> {
  const db = Reifydb.memory().build()
  await db.adminRoot('create namespace notes', {}, [])
  await db.adminRoot('create table notes::rows { id: int4, label: Option(utf8) }', {}, [])
  await db.adminRoot(
    'create procedure notes::add { id: int4, label: Option(utf8) } as { insert notes::rows [{ id: $id, label: $label }] returning { id } }',
    {},
    [],
  )
  return db
}

describe('procedure with an Option(utf8) parameter', () => {
  it('binds a typed none and stores a none of Utf8', async () => {
    const db = await setup()

    await db.commandRoot(ADD, { id: 1, label: Option.none('Utf8') }, [])
    const [rows] = await db.queryRoot(STORED, { id: 1 }, [LABEL])

    expect(rows).toEqual([{ label: Option.none('Utf8') }])
  })

  it('binds a some and stores the value', async () => {
    const db = await setup()

    await db.commandRoot(ADD, { id: 2, label: Option.some('x') }, [])
    const [rows] = await db.queryRoot(STORED, { id: 2 }, [LABEL])

    expect(rows).toEqual([{ label: Option.some('x') }])
  })

  it('rejects null for the parameter in the encoder before the bridge is reached', async () => {
    const db = await setup()

    expect(() => db.commandRoot(ADD, { id: 3, label: null }, [])).toThrow(
      'parameter $label is null or undefined, use Option.none(inner)',
    )
  })
})

const ECHO = 'MAP { v: $v }'
const ONE = Shape.object({ v: Shape.option(Shape.int4()) })

describe('Option parameters echoed through the bridge', () => {
  it('echoes a none of Int4', async () => {
    const db = Reifydb.memory().build()

    const [rows] = await db.queryRoot(ECHO, { v: Option.none('Int4') }, [ONE])

    expect(rows).toEqual([{ v: Option.none('Int4') }])
  })
})
