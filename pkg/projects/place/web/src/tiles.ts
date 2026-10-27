// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Uint2Value } from '@reifydb/core'
import type { PlaceClient } from './client'
import { bytes, numeric, version } from './client'
import { TILES_PER_SIDE } from './geometry'
import { decodeTile } from './pixels'

export const CHUNK_ROWS = 8

export const TILE_RQL = 'from place::tile filter { tile in $keys } map { tile, pixels, #commit_version }'

type Row = Record<string, unknown>

export interface TileSink {
  put(tx: number, ty: number, bytes: Uint8Array, version: bigint): void
}

export class TileStream {
  private subscriptionIds: string[] = []
  private queue: Promise<void> = Promise.resolve()

  constructor(
    private readonly client: PlaceClient,
    private readonly sink: TileSink,
    private readonly onClosed: () => void,
  ) {}

  start(): Promise<void> {
    return this.enqueue(() => this.subscribe())
  }

  close(): Promise<void> {
    return this.enqueue(() => this.unsubscribe())
  }

  private enqueue(run: () => Promise<void>): Promise<void> {
    this.queue = this.queue.then(run, run)
    return this.queue
  }

  private async subscribe(): Promise<void> {
    const chunks = []
    for (let row = 0; row < TILES_PER_SIDE; row += CHUNK_ROWS) {
      chunks.push({
        rql: TILE_RQL,
        params: { keys: chunkKeys(row) },
        config: { hydration: { enabled: true } },
        callbacks: {
          onInsert: (rows: Row[]) => void this.receive(rows),
          onUpdate: (rows: Row[]) => void this.receive(rows),
          onClosed: this.onClosed,
        },
      })
    }
    this.subscriptionIds = await this.client.subscribe(chunks)
  }

  private async unsubscribe(): Promise<void> {
    const subscriptionIds = this.subscriptionIds
    this.subscriptionIds = []
    if (subscriptionIds.length > 0) await this.client.unsubscribe(subscriptionIds)
  }

  private async receive(rows: Row[]): Promise<void> {
    for (const row of rows) {
      const tile = numeric(row.tile)
      const pixels = await decodeTile(bytes(row.pixels))
      this.sink.put(tile % TILES_PER_SIDE, Math.floor(tile / TILES_PER_SIDE), pixels, version(row['#commit_version']))
    }
  }
}

function chunkKeys(firstRow: number): Uint2Value[] {
  return Array.from({ length: CHUNK_ROWS * TILES_PER_SIDE }, (_, i) => new Uint2Value(firstRow * TILES_PER_SIDE + i))
}
