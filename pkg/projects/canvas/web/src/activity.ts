// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { PALETTE } from './palette'

export const ACTIVITY_LIMIT = 50

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

export class Activity {
  private edits: Edit[] = []
  private next = 0
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
    this.edits = [{ id: this.next++, name, tx: edit[0], ty: edit[1], px, color, at: this.now() }, ...this.edits].slice(0, ACTIVITY_LIMIT)
    for (const listener of this.listeners) listener()
  }

  latest(): readonly Edit[] {
    return this.edits
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }
}
