// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { storeClient } from '@reifydb/reifydb'
import { type Retry, commitPending, commitPixels } from '@/commit'
import { MAX_PENDING } from '@/geometry'
import { Pending } from '@/pending'
import { ANONYMOUS, guestClient, loadBackend, type BridgeClient, type TestDb, type TestFactory } from '../support/backend'
import { colorAt, markReady, seedTile } from '../support/seed'

let create: TestFactory
let db: TestDb
let client: BridgeClient

beforeAll(() => {
  create = loadBackend()
})

beforeEach(async () => {
  db = create()
  await seedTile(db, 0, 0)
  await markReady(db)
  client = await guestClient(db)
})

describe('commit', () => {
  it('writes every pending pixel and then empties pending', async () => {
    // A committed pixel left in pending would be sent again and hide later changes from other users.
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 12 })
    pending.add({ x: 2, y: 1, color: 13 })

    const result = await commitPending(client, pending)

    expect(result).toEqual({ ok: true, committed: 2 })
    expect(await colorAt(db, 1, 1)).toBe(12)
    expect(await colorAt(db, 2, 1)).toBe(13)
    expect(pending.size).toBe(0)
  })

  it('keeps every pending pixel when the server refuses the commit', async () => {
    // A refused commit must never drop the user's drawing; anonymous may not paint, so the engine refuses.
    const painter = storeClient(db, { identity: ANONYMOUS })
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 12 })
    pending.add({ x: 2, y: 1, color: 13 })

    const command = vi.spyOn(painter, 'command')
    const result = await commitPending(painter, pending)

    expect(result.ok).toBe(false)
    expect(command).toHaveBeenCalledTimes(1)
    if (!result.ok) expect(result.message).toContain('POLICY_001')
    expect(pending.entries()).toEqual([
      { x: 1, y: 1, color: 12 },
      { x: 2, y: 1, color: 13 },
    ])
    expect(await colorAt(db, 1, 1)).toBe(0)
  })

  it('refuses a pixel past the cap before anything is sent', async () => {
    // The server rejects more than 4,096 pixels whole, so the client must stop at the cap instead of sending a doomed commit.
    const pending = new Pending()
    for (let i = 0; i < MAX_PENDING; i++) expect(pending.add({ x: i % 64, y: Math.floor(i / 64), color: 5 })).toBe(true)
    expect(pending.add({ x: 0, y: MAX_PENDING / 64, color: 5 })).toBe(false)
    expect(pending.size).toBe(MAX_PENDING)

    const command = vi.spyOn(client, 'command')
    const result = await commitPending(client, pending)

    expect(result).toEqual({ ok: true, committed: MAX_PENDING })
    expect(command).toHaveBeenCalledTimes(1)
    expect(await colorAt(db, 63, 15)).toBe(5)
  })

  it('keeps a pixel repainted while its commit is in flight', async () => {
    // A recolor made during the call was never sent, so settling must not discard it.
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 12 })
    const original = client.command.bind(client)
    vi.spyOn(client, 'command').mockImplementation(async (rql, params, shapes) => {
      pending.add({ x: 1, y: 1, color: 27 })
      return original(rql, params, shapes)
    })

    const result = await commitPending(client, pending)

    expect(result).toEqual({ ok: true, committed: 1 })
    expect(await colorAt(db, 1, 1)).toBe(12)
    expect(pending.entries()).toEqual([{ x: 1, y: 1, color: 27 }])
  })

  it('refuses pixels outside the canvas', () => {
    // The server rejects out-of-canvas pixels and the whole commit with them, so pending must never hold one.
    const pending = new Pending()
    expect(pending.add({ x: 8192, y: 0, color: 1 })).toBe(false)
    expect(pending.add({ x: 0, y: 8192, color: 1 })).toBe(false)
    expect(pending.add({ x: -1, y: 0, color: 1 })).toBe(false)
    expect(pending.add({ x: 8191, y: 8191, color: 1 })).toBe(true)
  })

  it('refuses colors outside the 32-color palette', () => {
    // The server refuses a color past the palette and the whole commit with it, so pending must never hold one.
    const pending = new Pending()
    expect(pending.add({ x: 0, y: 0, color: 32 })).toBe(false)
    expect(pending.add({ x: 0, y: 0, color: -1 })).toBe(false)
    expect(pending.add({ x: 0, y: 0, color: 31 })).toBe(true)
  })
})

function conflict(): Error {
  return Object.assign(new Error('[TXN_001] transaction conflict'), { code: 'TXN_001' })
}

function recordingRetry(): Retry & { waits: number[] } {
  const waits: number[] = []
  return { retries: 5, baseDelayMs: 50, sleep: async (ms) => void waits.push(ms), random: () => 0.5, waits }
}

describe('commit retry', () => {
  it('retries a conflicted commit with growing jittered waits until it lands', async () => {
    // Two users on one tile conflict by design; without a retry the loser's pixels never land.
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 12 })
    const original = client.command.bind(client)
    const command = vi
      .spyOn(client, 'command')
      .mockRejectedValueOnce(conflict())
      .mockRejectedValueOnce(conflict())
      .mockImplementation(original)
    const retry = recordingRetry()

    const result = await commitPending(client, pending, retry)

    expect(result).toEqual({ ok: true, committed: 1 })
    expect(command).toHaveBeenCalledTimes(3)
    expect(retry.waits).toEqual([75, 150])
    expect(await colorAt(db, 1, 1)).toBe(12)
    expect(pending.size).toBe(0)
  })

  it('gives up after five retries and keeps every pending pixel', async () => {
    // An endless retry would hang the commit button; giving up must still never drop the drawing.
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 12 })
    const command = vi.spyOn(client, 'command').mockRejectedValue(conflict())
    const retry = recordingRetry()

    const result = await commitPending(client, pending, retry)

    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.message).toContain('TXN_001')
    expect(command).toHaveBeenCalledTimes(6)
    expect(retry.waits).toEqual([75, 150, 300, 600, 1200])
    expect(pending.entries()).toEqual([{ x: 1, y: 1, color: 12 }])
    expect(await colorAt(db, 1, 1)).toBe(0)
  })
})

describe('commitPixels', () => {
  it('writes the given pixels without a pending buffer', async () => {
    // The image flow sends straight from the stamp; nothing may route through pending.
    const result = await commitPixels(client, [{ x: 3, y: 1, color: 14 }, { x: 4, y: 1, color: 15 }])

    expect(result).toEqual({ ok: true, committed: 2 })
    expect(await colorAt(db, 3, 1)).toBe(14)
    expect(await colorAt(db, 4, 1)).toBe(15)
  })

  it('sends nothing for an empty list', async () => {
    // A fully transparent or off-canvas stamp must not cost a server round trip.
    const command = vi.spyOn(client, 'command')

    expect(await commitPixels(client, [])).toEqual({ ok: true, committed: 0 })
    expect(command).not.toHaveBeenCalled()
  })
})

describe('commitPending after the split', () => {
  it('does not notify pending when there is nothing to commit', async () => {
    // The old-pending commit before every image pick runs on an empty buffer most of the time; it must not redraw.
    const pending = new Pending()
    let calls = 0
    pending.subscribe(() => calls++)

    expect(await commitPending(client, pending)).toEqual({ ok: true, committed: 0 })
    expect(calls).toBe(0)
  })
})
