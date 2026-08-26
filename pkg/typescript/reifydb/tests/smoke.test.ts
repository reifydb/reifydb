// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { Reifydb } from '../src/index'

const migrationsDir = resolve(fileURLToPath(import.meta.url), '../fixtures/migrations')

describe('smoke', () => {
  it('opens, migrates, inserts, and queries a row through the real engine', async () => {
    const db = Reifydb.memory().with_migrations({ dir: migrationsDir }).build()

    await db.command_root('insert smoke::items [{ id: 1, label: "hello" }]')
    const result = await db.query_root('from smoke::items map { id, label }')

    expect(JSON.parse(result)).toEqual([{ id: '1', label: 'hello' }])
  })

  it('query_root on an empty table returns no rows', async () => {
    const db = Reifydb.memory().with_migrations({ dir: migrationsDir }).build()

    const result = await db.query_root('from smoke::items map { id, label }')

    expect(JSON.parse(result)).toEqual([])
  })
})
