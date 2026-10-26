// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { PALETTE } from './palette'
import { TILE_BYTES } from './pixels'

export const ACTIVITY_LIMIT = 50

export interface Edit {
  id: number
  tx: number
  ty: number
  px: number
  color: number
  at: number
}

export class Activity {
  private edits: Edit[] = []
  private next = 0
  private readonly listeners = new Set<() => void>()
  private readonly counts = new Uint16Array(PALETTE.length)

  constructor(private readonly now: () => number = Date.now) {}

  put(tx: number, ty: number, before: Uint8Array | undefined, after: Uint8Array): void {
    if (!before) return
    if (before.length !== TILE_BYTES || after.length !== TILE_BYTES) throw new Error(`tile (${tx},${ty}) is not ${TILE_BYTES} bytes`)
    this.counts.fill(0)
    let px = 0
    for (let i = 0; i < TILE_BYTES; i++) {
      if (before[i] === after[i]) continue
      px++
      this.counts[after[i]]++
    }
    if (px === 0) return
    let color = 0
    for (let c = 1; c < this.counts.length; c++) if (this.counts[c] > this.counts[color]) color = c
    this.edits = [{ id: this.next++, tx, ty, px, color, at: this.now() }, ...this.edits].slice(0, ACTIVITY_LIMIT)
    for (const listener of this.listeners) listener()
  }

  latest(): readonly Edit[] {
    return this.edits
  }

  pixelsSince(ms: number): number {
    const from = this.now() - ms
    return this.edits.filter((edit) => edit.at >= from).reduce((total, edit) => total + edit.px, 0)
  }

  editsSince(ms: number): number {
    const from = this.now() - ms
    return this.edits.filter((edit) => edit.at >= from).length
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }
}
