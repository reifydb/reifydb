// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { commitPending } from '@/commit'
import { tileKey } from '@/geometry'
import { Pending } from '@/pending'
import { PixelBuffer } from '@/pixels'
import { TileStream } from '@/tiles'
import { anonymousClient, loadBackend, type BridgeClient, type TestDb, type TestFactory } from '../support/backend'
import { markReady, seedTile } from '../support/seed'

let create: TestFactory
let db: TestDb
let client: BridgeClient
let buffer: PixelBuffer
let stream: TileStream

beforeAll(() => {
  create = loadBackend()
})

beforeEach(async () => {
  db = create()
  for (const [tx, ty] of [
    [0, 0],
    [1, 0],
    [2, 0],
  ]) {
    await seedTile(db, tx, ty)
  }
  await markReady(db)
  client = anonymousClient(db)
  buffer = new PixelBuffer()
  stream = new TileStream(client, (x, y, color) => buffer.set(x, y, color))
})

async function paint(x: number, y: number, color: number) {
  const pending = new Pending()
  pending.add({ x, y, color })
  const result = await commitPending(client, pending)
  expect(result.ok).toBe(true)
  await client.caughtUp()
}

function tilesOf(items: Array<{ params: { tx: { value: number }; ty: { value: number } } }>) {
  return items.map((item) => tileKey(item.params.tx.value, item.params.ty.value))
}

describe('tile stream', () => {
  it('loads the snapshot of a visible tile and then shows a later commit', async () => {
    // A visible tile must show the stored colors and then every commit, or users paint over pixels they cannot see.
    await paint(10, 10, 12)
    await stream.show(new Set([tileKey(0, 0)]))
    expect(buffer.color(10, 10)).toBe(12)
    expect(buffer.color(63, 63)).toBe(0)
    expect(buffer.color(64, 0)).toBeUndefined()

    await paint(3, 4, 99)

    expect(buffer.color(3, 4)).toBe(99)
  })

  it('stops streaming a tile once it leaves the screen', async () => {
    // A hidden tile must release its subscription, or panning across the canvas leaks one subscription per tile.
    const unsubscribe = vi.spyOn(client, 'batchUnsubscribe')
    await stream.show(new Set([tileKey(0, 0)]))
    await stream.show(new Set([tileKey(2, 0)]))

    expect(unsubscribe).toHaveBeenCalledTimes(1)
    expect(stream.subscribed()).toEqual([tileKey(2, 0)])
    await paint(5, 5, 40)
    expect(buffer.color(5, 5)).toBe(0)
  })

  it('subscribes and loads only the tiles that became visible', async () => {
    // Panning one column must fetch one column, not the whole screen again.
    const subscribe = vi.spyOn(client, 'batchSubscribe')
    const query = vi.spyOn(client, 'query')
    await stream.show(new Set([tileKey(0, 0), tileKey(1, 0)]))
    subscribe.mockClear()
    query.mockClear()

    await stream.show(new Set([tileKey(1, 0), tileKey(2, 0)]))

    expect(subscribe).toHaveBeenCalledTimes(1)
    expect(tilesOf(subscribe.mock.calls[0][0] as never)).toEqual([tileKey(2, 0)])
    expect(query).toHaveBeenCalledTimes(1)
    expect(stream.subscribed().sort()).toEqual([tileKey(0, 0), tileKey(1, 0), tileKey(2, 0)].sort())
  })

  it('replays a change that arrives after the snapshot was read', async () => {
    // A commit landing between snapshot and draw must win, or the tile shows a stale color until the next change.
    const original = client.query.bind(client)
    vi.spyOn(client, 'query').mockImplementationOnce(async (rql, params, shapes) => {
      const snapshot = await original(rql, params, shapes)
      await paint(7, 7, 55)
      return snapshot
    })

    await stream.show(new Set([tileKey(0, 0)]))

    expect(buffer.color(7, 7)).toBe(55)
  })

  it('handles changes delivered through onInsert as well as onUpdate', async () => {
    // With hydration off the engine delivers changed rows through onInsert; ignoring it would freeze every tile.
    const subscribe = vi.spyOn(client, 'batchSubscribe')
    await stream.show(new Set([tileKey(0, 0)]))
    const callbacks = subscribe.mock.calls[0][0][0].callbacks
    expect(typeof callbacks.onInsert).toBe('function')
    expect(typeof callbacks.onUpdate).toBe('function')

    await paint(1, 2, 3)
    expect(buffer.color(1, 2)).toBe(3)
  })
})
