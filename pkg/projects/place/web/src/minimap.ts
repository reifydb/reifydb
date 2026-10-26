// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { CANVAS_SIZE, TILE_SIZE } from './geometry'
import { PALETTE } from './palette'
import { TILE_BYTES } from './pixels'
import type { Point } from './tools'

export const MINIMAP_SIZE = 512
export const MINIMAP_CSS_SIZE = 192

const BLOCK = CANVAS_SIZE / MINIMAP_SIZE
const BLOCKS_PER_TILE = TILE_SIZE / BLOCK

export interface Rect {
  x: number
  y: number
  width: number
  height: number
}

export class Minimap {
  readonly pixels = new Uint8ClampedArray(MINIMAP_SIZE * MINIMAP_SIZE * 4)
  private dirty: Rect | null = null
  private readonly listeners = new Set<() => void>()
  private readonly counts = new Uint16Array(PALETTE.length)

  put(tx: number, ty: number, bytes: Uint8Array): void {
    if (bytes.length !== TILE_BYTES) throw new Error(`tile (${tx},${ty}) has ${bytes.length} bytes, expected ${TILE_BYTES}`)
    for (let by = 0; by < BLOCKS_PER_TILE; by++) {
      for (let bx = 0; bx < BLOCKS_PER_TILE; bx++) {
        const rgb = PALETTE[this.mode(bytes, bx, by)]
        const offset = ((ty * BLOCKS_PER_TILE + by) * MINIMAP_SIZE + tx * BLOCKS_PER_TILE + bx) * 4
        this.pixels[offset] = (rgb >> 16) & 0xff
        this.pixels[offset + 1] = (rgb >> 8) & 0xff
        this.pixels[offset + 2] = rgb & 0xff
        this.pixels[offset + 3] = 0xff
      }
    }
    this.mark({ x: tx * BLOCKS_PER_TILE, y: ty * BLOCKS_PER_TILE, width: BLOCKS_PER_TILE, height: BLOCKS_PER_TILE })
  }

  takeDirty(): Rect | null {
    const dirty = this.dirty
    this.dirty = null
    return dirty
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private mode(bytes: Uint8Array, bx: number, by: number): number {
    this.counts.fill(0)
    for (let y = by * BLOCK; y < (by + 1) * BLOCK; y++) {
      for (let x = bx * BLOCK; x < (bx + 1) * BLOCK; x++) this.counts[bytes[y * TILE_SIZE + x]]++
    }
    let best = 0
    for (let color = 1; color < this.counts.length; color++) if (this.counts[color] > this.counts[best]) best = color
    return best
  }

  private mark(rect: Rect): void {
    const held = this.dirty
    if (held === null) {
      this.dirty = rect
      for (const listener of this.listeners) listener()
      return
    }
    const right = Math.max(held.x + held.width, rect.x + rect.width)
    const bottom = Math.max(held.y + held.height, rect.y + rect.height)
    held.x = Math.min(held.x, rect.x)
    held.y = Math.min(held.y, rect.y)
    held.width = right - held.x
    held.height = bottom - held.y
  }
}

export function minimapToCanvas(px: number, py: number): Point {
  const scale = CANVAS_SIZE / MINIMAP_CSS_SIZE
  return { x: px * scale, y: py * scale }
}

export function viewBox(view: { zoom: number; x: number; y: number }, size: { width: number; height: number }): Rect {
  const scale = MINIMAP_CSS_SIZE / CANVAS_SIZE / view.zoom
  return { x: 0 - view.x * scale, y: 0 - view.y * scale, width: size.width * scale, height: size.height * scale }
}
