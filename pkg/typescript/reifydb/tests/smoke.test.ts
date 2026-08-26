// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { openWithMigrationsDir } from '../src/index'

const migrationsDir = resolve(fileURLToPath(import.meta.url), '../fixtures/migrations')

describe('smoke', () => {
  it('opens, migrates, inserts, and queries a row through the real engine', async () => {
    const db = openWithMigrationsDir(migrationsDir)

    await db.commandRoot('insert smoke::items [{ id: 1, label: "hello" }]')
    const result = await db.queryRoot('from smoke::items map { id, label }')

    expect(JSON.parse(result)).toEqual([{ id: '1', label: 'hello' }])
  })

  it('queryRoot on an empty table returns no rows', async () => {
    const db = openWithMigrationsDir(migrationsDir)

    const result = await db.queryRoot('from smoke::items map { id, label }')

    expect(JSON.parse(result)).toEqual([])
  })

  it('rejects when the migrations directory does not exist', () => {
    // must surface as a catchable error, not crash the process or hang the promise
    expect(() => openWithMigrationsDir('/no/such/directory')).toThrow()
  })
})
