// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Uint1Value } from '@reifydb/core'
import type { PlaceClient } from './client'
import { bytes, version } from './client'
import { type TileKey, parseTileKey } from './geometry'
import { decodeTile } from './pixels'

export const TILE_RQL =
  'from place::tile filter { tile_x == $tx and tile_y == $ty } map { tile_x, tile_y, pixels, #commit_version }'

type Row = Record<string, unknown>

export interface TileSink {
  put(tx: number, ty: number, bytes: Uint8Array, version: bigint): void
  drop(tx: number, ty: number): void
}

export class TileStream {
  private readonly tiles = new Set<TileKey>()
  private readonly batches = new Map<string, Set<TileKey>>()
  private visible = new Set<TileKey>()
  private queue: Promise<void> = Promise.resolve()

  constructor(
    private readonly client: PlaceClient,
    private readonly sink: TileSink,
  ) {}

  subscribed(): TileKey[] {
    return [...this.tiles]
  }

  show(visible: Set<TileKey>): Promise<void> {
    const run = () => this.apply(new Set(visible))
    this.queue = this.queue.then(run, run)
    return this.queue
  }

  close(): Promise<void> {
    return this.show(new Set())
  }

  private async apply(visible: Set<TileKey>): Promise<void> {
    this.visible = visible
    await this.dropHidden()
    const fresh = [...visible].filter((key) => !this.tiles.has(key))
    if (fresh.length === 0) return
    for (const key of fresh) this.tiles.add(key)
    const batch = await this.client.batchSubscribe(
      fresh.map((key) => {
        const [tx, ty] = parseTileKey(key)
        return {
          rql: TILE_RQL,
          params: params(tx, ty),
          config: { hydration: { enabled: false } },
          callbacks: {
            onInsert: (rows: Row[]) => void this.receive(key, rows),
            onUpdate: (rows: Row[]) => void this.receive(key, rows),
            onResubscribe: () => void this.load(key),
          },
        }
      }),
    )
    this.batches.set(batch.batchId, new Set(fresh))
    await Promise.all(fresh.map((key) => this.load(key)))
  }

  private async dropHidden(): Promise<void> {
    for (const [batchId, keys] of this.batches) {
      if ([...keys].some((key) => this.visible.has(key))) continue
      this.batches.delete(batchId)
      for (const key of keys) {
        this.tiles.delete(key)
        const [tx, ty] = parseTileKey(key)
        this.sink.drop(tx, ty)
      }
      await this.client.batchUnsubscribe(batchId)
    }
  }

  private async load(key: TileKey): Promise<void> {
    const [tx, ty] = parseTileKey(key)
    const result = await this.client.query(TILE_RQL, params(tx, ty), [])
    await this.receive(key, (result?.[0] ?? []) as Row[])
  }

  private async receive(key: TileKey, rows: Row[]): Promise<void> {
    const [tx, ty] = parseTileKey(key)
    for (const row of rows) {
      const at = version(row['#commit_version'])
      const pixels = await decodeTile(bytes(row.pixels))
      if (this.tiles.has(key)) this.sink.put(tx, ty, pixels, at)
    }
  }
}

function params(tx: number, ty: number) {
  return { tx: new Uint1Value(tx), ty: new Uint1Value(ty) }
}
