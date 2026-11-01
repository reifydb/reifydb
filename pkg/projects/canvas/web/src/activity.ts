// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { PALETTE } from './palette'

export const ACTIVITY_LIMIT = 50

export const STREAK_LIMIT = 100

export const STREAK_MS = 10_000

export const EDIT_RQL = 'from canvas::edit map { name, pixels, painted }'

export interface Edit {
  id: number
  name: string
  tx: number
  ty: number
  px: number
  color: number
  at: number
}

export interface Streak {
  id: number
  name: string
  px: number
  tiles: readonly (readonly [number, number])[]
  tx: number
  ty: number
  color: number
  at: number
  counts: Uint32Array
}

export class Activity {
  private edits: Edit[] = []
  private runs: Streak[] = []
  private next = 0
  private nextStreak = 0
  private readonly listeners = new Set<() => void>()
  private readonly counts = new Uint16Array(PALETTE.length)

  constructor(private readonly now: () => number = Date.now) {}

  put(name: string, edit: Uint8Array, px: number): void {
    if (edit.length < 2 || (edit.length - 2) % 3 !== 0) throw new Error(`edit of ${name} has ${edit.length} bytes, expected 2 + 3n`)
    if (px === 0) return
    this.counts.fill(0)
    for (let i = 4; i < edit.length; i += 3) this.counts[edit[i]]++
    let color = 0
    for (let c = 1; c < this.counts.length; c++) if (this.counts[c] > this.counts[color]) color = c
    const at = this.now()
    const [tx, ty] = [edit[0], edit[1]]
    this.edits = [{ id: this.next++, name, tx, ty, px, color, at }, ...this.edits].slice(0, ACTIVITY_LIMIT)
    this.extend(name, tx, ty, px, at)
    for (const listener of this.listeners) listener()
  }

  latest(): readonly Edit[] {
    return this.edits
  }

  streaks(): readonly Streak[] {
    return this.runs
  }

  private extend(name: string, tx: number, ty: number, px: number, at: number): void {
    const top = this.runs[0]
    const joins = top !== undefined && top.name === name && at - top.at <= STREAK_MS
    const counts = joins ? top.counts.slice() : new Uint32Array(PALETTE.length)
    for (let c = 0; c < this.counts.length; c++) counts[c] += this.counts[c]
    let color = 0
    for (let c = 1; c < counts.length; c++) if (counts[c] > counts[color]) color = c
    const seen = joins && top.tiles.some(([x, y]) => x === tx && y === ty)
    const tiles = joins ? (seen ? top.tiles : [...top.tiles, [tx, ty] as const]) : [[tx, ty] as const]
    const streak = { id: joins ? top.id : this.nextStreak++, name, px: (joins ? top.px : 0) + px, tiles, tx, ty, color, at, counts }
    this.runs = joins ? [streak, ...this.runs.slice(1)] : [streak, ...this.runs].slice(0, STREAK_LIMIT)
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }
}
