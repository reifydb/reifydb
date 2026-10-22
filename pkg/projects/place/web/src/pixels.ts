// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { CANVAS_SIZE, inCanvas } from './geometry'

export interface DirtyRect {
  left: number
  top: number
  right: number
  bottom: number
}

export class PixelBuffer {
  readonly colors = new Uint8Array(CANVAS_SIZE * CANVAS_SIZE)
  private readonly loaded = new Uint8Array(CANVAS_SIZE * CANVAS_SIZE)
  private dirty: DirtyRect | null = null
  private readonly listeners = new Set<() => void>()

  set(x: number, y: number, color: number): void {
    if (!inCanvas(x, y)) return
    const i = y * CANVAS_SIZE + x
    this.colors[i] = color
    this.loaded[i] = 1
    this.mark(x, y)
  }

  color(x: number, y: number): number | undefined {
    if (!inCanvas(x, y)) return undefined
    const i = y * CANVAS_SIZE + x
    return this.loaded[i] === 1 ? this.colors[i] : undefined
  }

  takeDirty(): DirtyRect | null {
    const dirty = this.dirty
    this.dirty = null
    return dirty
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private mark(x: number, y: number): void {
    const wasClean = this.dirty === null
    if (this.dirty === null) {
      this.dirty = { left: x, top: y, right: x + 1, bottom: y + 1 }
    } else {
      this.dirty.left = Math.min(this.dirty.left, x)
      this.dirty.top = Math.min(this.dirty.top, y)
      this.dirty.right = Math.max(this.dirty.right, x + 1)
      this.dirty.bottom = Math.max(this.dirty.bottom, y + 1)
    }
    if (wasClean) for (const listener of this.listeners) listener()
  }
}
