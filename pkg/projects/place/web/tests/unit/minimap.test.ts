// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { MINIMAP_SIZE, Minimap, minimapToCanvas, viewBox } from '@/minimap'
import { PALETTE } from '@/palette'
import { TILE_BYTES } from '@/pixels'

const RED = 2
const BLUE = 13

function tile(color: number): Uint8Array {
  return new Uint8Array(TILE_BYTES).fill(color)
}

function rgba(minimap: Minimap, x: number, y: number): number[] {
  const offset = (y * MINIMAP_SIZE + x) * 4
  return Array.from(minimap.pixels.subarray(offset, offset + 4))
}

function opaque(color: number): number[] {
  const rgb = PALETTE[color]
  return [(rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff, 0xff]
}

function painted(minimap: Minimap): Array<[number, number]> {
  const hits: Array<[number, number]> = []
  for (let y = 0; y < MINIMAP_SIZE; y++) {
    for (let x = 0; x < MINIMAP_SIZE; x++) if (minimap.pixels[(y * MINIMAP_SIZE + x) * 4 + 3] !== 0) hits.push([x, y])
  }
  return hits
}

describe('Minimap.put', () => {
  it('writes tile (3,5) to minimap x 12..15, y 20..23 and nowhere else', () => {
    // Swapping tx and ty would draw every tile mirrored across the diagonal.
    const minimap = new Minimap()
    minimap.put(3, 5, tile(RED))

    const hits = painted(minimap)
    expect(hits).toHaveLength(16)
    for (const [x, y] of hits) {
      expect(x).toBeGreaterThanOrEqual(12)
      expect(x).toBeLessThanOrEqual(15)
      expect(y).toBeGreaterThanOrEqual(20)
      expect(y).toBeLessThanOrEqual(23)
    }
  })

  it('stays transparent where no tile has arrived', () => {
    // Unloaded must not look like painted white, which is palette index 0.
    const minimap = new Minimap()
    minimap.put(0, 0, tile(0))

    expect(rgba(minimap, 0, 0)).toEqual(opaque(0))
    expect(rgba(minimap, 4, 0)).toEqual([0, 0, 0, 0])
  })

  it('writes the palette colour as opaque rgba', () => {
    // A wrong channel order or a missing alpha shows wrong or invisible colours.
    const minimap = new Minimap()
    minimap.put(0, 0, tile(BLUE))

    expect(rgba(minimap, 0, 0)).toEqual(opaque(BLUE))
  })

  it('picks the majority colour of a 16x16 block when it wins by one pixel', () => {
    // An off-by-one in the block loop drops the edge row or column and flips the winner.
    const bytes = tile(0)
    let red = 0
    for (let y = 0; y < 16 && red < 129; y++) {
      for (let x = 0; x < 16 && red < 129; x++, red++) bytes[y * 64 + x] = RED
    }
    const minimap = new Minimap()
    minimap.put(0, 0, bytes)

    expect(rgba(minimap, 0, 0)).toEqual(opaque(RED))
    expect(rgba(minimap, 1, 0)).toEqual(opaque(0))
  })

  it('breaks an exact 128/128 tie toward the lowest palette index', () => {
    // A non-strict comparison would let the higher index win and make the colour depend on loop order.
    const bytes = tile(BLUE)
    for (let y = 0; y < 8; y++) for (let x = 0; x < 16; x++) bytes[y * 64 + x] = RED
    const minimap = new Minimap()
    minimap.put(0, 0, bytes)

    expect(rgba(minimap, 0, 0)).toEqual(opaque(RED))
  })

  it('reads the block from the block in the bottom-right corner of the tile', () => {
    // Block offsets must step by 16 px inside the tile, not by 4.
    const bytes = tile(0)
    for (let y = 48; y < 64; y++) for (let x = 48; x < 64; x++) bytes[y * 64 + x] = RED
    const minimap = new Minimap()
    minimap.put(0, 0, bytes)

    expect(rgba(minimap, 3, 3)).toEqual(opaque(RED))
    expect(rgba(minimap, 2, 3)).toEqual(opaque(0))
    expect(rgba(minimap, 3, 2)).toEqual(opaque(0))
  })
})

describe('Minimap.takeDirty', () => {
  it('returns the union of two tiles and then nothing', () => {
    // A lost or unreset dirty rect leaves stale minimap pixels on screen or redraws forever.
    const minimap = new Minimap()
    minimap.put(3, 5, tile(RED))
    minimap.put(10, 2, tile(RED))

    expect(minimap.takeDirty()).toEqual({ x: 12, y: 8, width: 32, height: 16 })
    expect(minimap.takeDirty()).toBeNull()
  })

  it('notifies once per burst, and again after the dirty rect was taken', () => {
    // Notifying per tile would schedule thousands of frames during hydration.
    const minimap = new Minimap()
    let calls = 0
    minimap.subscribe(() => calls++)

    minimap.put(0, 0, tile(RED))
    minimap.put(1, 0, tile(RED))
    expect(calls).toBe(1)

    minimap.takeDirty()
    minimap.put(2, 0, tile(RED))
    expect(calls).toBe(2)
  })
})

describe('minimapToCanvas', () => {
  it('maps the centre of the 192 px panel to the centre of the canvas', () => {
    // Scaling by the 512 buffer size instead of the 192 css size lands far off target.
    expect(minimapToCanvas(96, 96)).toEqual({ x: 4096, y: 4096 })
    expect(minimapToCanvas(192, 0)).toEqual({ x: 8192, y: 0 })
  })
})

describe('viewBox', () => {
  it('shrinks the box with zoom and offsets it by the panned view', () => {
    // At zoom 4 a 1920x1080 screen covers 480x270 canvas px, which is 11.25x6.33 panel px.
    const box = viewBox({ zoom: 4, x: -4096 * 4, y: -2048 * 4 }, { width: 1920, height: 1080 })

    expect(box.x).toBeCloseTo(96)
    expect(box.y).toBeCloseTo(48)
    expect(box.width).toBeCloseTo(11.25)
    expect(box.height).toBeCloseTo(6.328, 2)
  })

  it('covers the canvas span at zoom 1 with no pan', () => {
    // The box must use the screen size, not the canvas size.
    expect(viewBox({ zoom: 1, x: 0, y: 0 }, { width: 8192, height: 4096 })).toEqual({ x: 0, y: 0, width: 192, height: 96 })
  })
})
