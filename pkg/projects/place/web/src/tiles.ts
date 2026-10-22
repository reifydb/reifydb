// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Uint1Value } from '@reifydb/core'
import type { PlaceClient } from './client'
import { numeric } from './client'
import { type TileKey, parseTileKey } from './geometry'

export const TILE_RQL = 'from place::live filter { tile_x == $tx and tile_y == $ty }'

type Row = Record<string, unknown>
type PixelSink = (x: number, y: number, color: number) => void

interface TileState {
  batchId: string | null
  buffer: Row[] | null
}

export class TileStream {
  private readonly tiles = new Map<TileKey, TileState>()
  private readonly batches = new Map<string, Set<TileKey>>()
  private visible = new Set<TileKey>()
  private queue: Promise<void> = Promise.resolve()

  constructor(
    private readonly client: PlaceClient,
    private readonly sink: PixelSink,
  ) {}

  subscribed(): TileKey[] {
    return [...this.tiles.keys()]
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
    for (const key of fresh) this.tiles.set(key, { batchId: null, buffer: [] })
    const batch = await this.client.batchSubscribe(
      fresh.map((key) => {
        const [tx, ty] = parseTileKey(key)
        return {
          rql: TILE_RQL,
          params: params(tx, ty),
          config: { hydration: { enabled: false } },
          callbacks: {
            onInsert: (rows: Row[]) => this.receive(key, rows),
            onUpdate: (rows: Row[]) => this.receive(key, rows),
            onResubscribe: () => void this.reload(key),
          },
        }
      }),
    )
    this.batches.set(batch.batchId, new Set(fresh))
    for (const key of fresh) {
      const state = this.tiles.get(key)
      if (state) state.batchId = batch.batchId
    }
    await Promise.all(fresh.map((key) => this.load(key)))
  }

  private async dropHidden(): Promise<void> {
    for (const [batchId, keys] of this.batches) {
      if ([...keys].some((key) => this.visible.has(key))) continue
      this.batches.delete(batchId)
      for (const key of keys) this.tiles.delete(key)
      await this.client.batchUnsubscribe(batchId)
    }
  }

  private async load(key: TileKey): Promise<void> {
    const [tx, ty] = parseTileKey(key)
    const result = await this.client.query(TILE_RQL, params(tx, ty), [])
    const state = this.tiles.get(key)
    if (!state) return
    for (const row of (result?.[0] ?? []) as Row[]) this.draw(row)
    const buffered = state.buffer ?? []
    state.buffer = null
    for (const row of buffered) this.draw(row)
  }

  private async reload(key: TileKey): Promise<void> {
    const state = this.tiles.get(key)
    if (!state) return
    state.buffer = []
    await this.load(key)
  }

  private receive(key: TileKey, rows: Row[]): void {
    const state = this.tiles.get(key)
    if (!state) return
    if (state.buffer) state.buffer.push(...rows)
    else for (const row of rows) this.draw(row)
  }

  private draw(row: Row): void {
    this.sink(numeric(row.x), numeric(row.y), numeric(row.color))
  }
}

function params(tx: number, ty: number) {
  return { tx: new Uint1Value(tx), ty: new Uint1Value(ty) }
}
