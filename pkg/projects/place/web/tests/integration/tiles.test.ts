// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { commitPending } from '@/commit'
import { Pending } from '@/pending'
import { PixelBuffer, TILE_BYTES, transform } from '@/pixels'
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
  stream = new TileStream(client, {
    put: (tx, ty, bytes, version) => void buffer.put(tx, ty, bytes, version),
  })
})

async function paint(x: number, y: number, color: number) {
  const pending = new Pending()
  pending.add({ x, y, color })
  const result = await commitPending(client, pending)
  expect(result.ok).toBe(true)
  await client.caughtUp()
}

type Chunk = { params: { keys: Array<{ value: number }> }; config: { hydration: { enabled: boolean } } }

const range = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => from + i)

async function tileBlob(color: number) {
  return transform(new Uint8Array(TILE_BYTES).fill(color), new CompressionStream('deflate-raw'))
}

describe('tile stream', () => {
  it('loads the snapshot of a visible tile and then shows a later commit', async () => {
    // A visible tile must show the stored colors and then every commit, or users paint over pixels they cannot see.
    await paint(10, 10, 12)
    await stream.start()
    await vi.waitFor(() => {
      expect(buffer.color(10, 10)).toBe(12)
      expect(buffer.color(63, 63)).toBe(0)
      expect(buffer.color(64, 0)).toBe(0)
    })

    await paint(3, 4, 19)

    await vi.waitFor(() => expect(buffer.color(3, 4)).toBe(19))
  })

  it('keeps every tile streaming without any view', async () => {
    // Every tile stays live so a pan never shows stale pixels; a far tile must update with no view at all.
    await stream.start()
    await vi.waitFor(() => expect(buffer.color(130, 5)).toBe(0))

    await paint(130, 5, 20)

    await vi.waitFor(() => expect(buffer.color(130, 5)).toBe(20))
  })

  it('releases every chunk subscription on close', async () => {
    // A stream that keeps one chunk after close leaks a live subscription per page visit.
    const unsubscribe = vi.spyOn(client, 'batchUnsubscribe')
    await stream.start()

    await stream.close()

    expect(unsubscribe).toHaveBeenCalledTimes(16)
  })

  it('subscribes the canvas in 16 hydrated chunks, top-left first', async () => {
    // Each chunk must stay under the hydration row cap, the start view must load first, and no tile may be missed.
    const subscribe = vi.spyOn(client, 'batchSubscribe')
    const query = vi.spyOn(client, 'query')

    await stream.start()

    const chunks = subscribe.mock.calls.map((call) => (call[0] as unknown as Chunk[])[0])
    const keys = chunks.map((chunk) => chunk.params.keys.map((key) => Number(key.value)))
    expect(subscribe).toHaveBeenCalledTimes(16)
    expect(keys[0]).toEqual(range(0, 1024))
    expect(chunks.every((chunk) => chunk.config.hydration.enabled)).toBe(true)
    expect(keys.flat().sort((a, b) => a - b)).toEqual(range(0, 16384))
    expect(query).not.toHaveBeenCalled()
  })

  it('never lets an older snapshot row overwrite a newer change', async () => {
    // Snapshot and live rows can arrive in either order; routing must carry the version or a tile rolls back.
    const subscribe = vi.spyOn(client, 'batchSubscribe')
    await stream.start()
    const deliver = subscribe.mock.calls[0][0][0].callbacks.onInsert!

    deliver([
      { tile: 1000, pixels: await tileBlob(25), '#commit_version': 2n },
      { tile: 1000, pixels: await tileBlob(12), '#commit_version': 1n },
      { tile: 1001, pixels: await tileBlob(7), '#commit_version': 1n },
    ])

    await vi.waitFor(() => expect(buffer.color(6720, 448)).toBe(7))
    expect(buffer.color(6656, 448)).toBe(25)
  })

  it('leaves a tile without a stored row undefined', async () => {
    // Only stored tiles exist; inventing a blank one would hide a seeding bug behind white pixels.
    await stream.start()
    await vi.waitFor(() => expect(buffer.color(10, 10)).toBe(0))

    expect(buffer.color(200, 0)).toBeUndefined()
  })

  it('handles changes delivered through onInsert as well as onUpdate', async () => {
    // The engine delivers snapshot and changed rows through onInsert; ignoring it would leave every tile blank.
    const subscribe = vi.spyOn(client, 'batchSubscribe')
    await stream.start()
    const callbacks = subscribe.mock.calls[0][0][0].callbacks
    expect(typeof callbacks.onInsert).toBe('function')
    expect(typeof callbacks.onUpdate).toBe('function')

    await paint(1, 2, 3)
    await vi.waitFor(() => expect(buffer.color(1, 2)).toBe(3))
  })
})

describe('tile versions', () => {
  it('applies a tile only when its commit version is newer than the one held', () => {
    // Tile events replace the whole tile, so an out-of-order older one must be dropped, not drawn.
    const tiles = new PixelBuffer()
    const painted = (color: number) => new Uint8Array(64 * 64).fill(color)

    expect(tiles.put(0, 0, painted(5), 5n)).toBe(true)
    expect(tiles.put(0, 0, painted(4), 4n)).toBe(false)
    expect(tiles.put(0, 0, painted(9), 5n)).toBe(false)
    expect(tiles.color(0, 0)).toBe(5)
    expect(tiles.put(0, 0, painted(6), 6n)).toBe(true)
    expect(tiles.color(63, 63)).toBe(6)
  })
})
