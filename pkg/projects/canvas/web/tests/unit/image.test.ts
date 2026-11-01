// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { MAX_PENDING } from '@/geometry'
import { convert, fitSize, preShrinkSize, stampPixels } from '@/image'
import { PALETTE } from '@/palette'

function image(pixels: [number, number, number, number][]): Uint8ClampedArray {
  return new Uint8ClampedArray(pixels.flat())
}

describe('fitSize', () => {
  it('keeps an image that already fits one commit', () => {
    // Small art must land pixel for pixel, never scaled.
    expect(fitSize(50, 40)).toEqual({ width: 50, height: 40 })
    expect(fitSize(64, 64)).toEqual({ width: 64, height: 64 })
  })

  it('shrinks to at most one commit and keeps the shape', () => {
    // A bigger result would be refused by the pending buffer and the server cap.
    const { width, height } = fitSize(300, 200)
    expect(width * height).toBeLessThanOrEqual(MAX_PENDING)
    expect({ width, height }).toEqual({ width: 78, height: 52 })
    expect(fitSize(4000, 4000)).toEqual({ width: 64, height: 64 })
  })

  it('caps extreme strips at one commit', () => {
    // Without the clamp a 10000x1 strip floors its height to 0, is raised to 1, and keeps 6400 wide.
    for (const [w, h] of [
      [10000, 1],
      [1, 10000],
      [100000, 3],
    ]) {
      const { width, height } = fitSize(w, h)
      expect(width).toBeGreaterThanOrEqual(1)
      expect(height).toBeGreaterThanOrEqual(1)
      expect(width * height).toBeLessThanOrEqual(MAX_PENDING)
    }
  })
})

describe('preShrinkSize', () => {
  it('pre-shrinks a big photo to four times the output per side', () => {
    // Memory must follow the output, while the final 4x4 average stays ours.
    expect(preShrinkSize(4000, 3000)).toEqual({ width: 292, height: 220 })
  })

  it('leaves sources that are already small enough alone', () => {
    // Pre-shrinking a small image would hand its pixels to browser smoothing for nothing.
    expect(preShrinkSize(100, 100)).toBeUndefined()
    expect(preShrinkSize(256, 256)).toBeUndefined()
  })

  it('never asks to grow a side', () => {
    // A strip shorter than four output rows must keep its real height, or decode would upscale it.
    const size = preShrinkSize(10000, 30)
    expect(size).toBeDefined()
    expect(size!.height).toBeLessThanOrEqual(30)
    expect(size!.width).toBeLessThanOrEqual(10000)
  })
})

describe('convert', () => {
  it('maps every palette colour to itself', () => {
    // A source already in palette colours must survive unchanged.
    const pixels = PALETTE.map((rgb) => [(rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff, 255] as [number, number, number, number])
    const stamp = convert(image(pixels), PALETTE.length, 1, { width: PALETTE.length, height: 1 })
    expect(stamp.cells).toEqual(PALETTE.map((_, index) => index))
  })

  it('picks colours by redmean, not plain RGB', () => {
    // Plain RGB distance sends this skin tone to pink (24); redmean must pick peach (27).
    const stamp = convert(image([[0xe0, 0xac, 0x90, 255]]), 1, 1, { width: 1, height: 1 })
    expect(stamp.cells).toEqual([27])
  })

  it('weights colour by opacity so transparent junk does not darken edges', () => {
    // A plain average mixes the hidden black in and gives dark red (1) instead of red (2).
    const stamp = convert(
      image([
        [255, 0, 0, 255],
        [0, 0, 0, 0],
      ]),
      2,
      1,
      { width: 1, height: 1 },
    )
    expect(stamp.cells).toEqual([2])
  })

  it('keeps a cell at exactly 50% opacity and skips one below', () => {
    // The threshold is "skip below 50%", so exactly half must still paint.
    const half = convert(
      image([
        [0, 0, 0, 255],
        [0, 0, 0, 0],
      ]),
      2,
      1,
      { width: 1, height: 1 },
    )
    expect(half.cells).toEqual([28])
    const third = convert(
      image([
        [0, 0, 0, 255],
        [0, 0, 0, 0],
        [0, 0, 0, 0],
      ]),
      3,
      1,
      { width: 1, height: 1 },
    )
    expect(third.cells).toEqual([undefined])
  })

  it('skips fully transparent cells without dividing by zero', () => {
    // A transparent logo background must leave the canvas untouched, not paint black.
    const stamp = convert(image([[0, 0, 0, 0]]), 1, 1, { width: 1, height: 1 })
    expect(stamp.cells).toEqual([undefined])
  })

  it('splits blocks on integer edges with every source pixel counted once', () => {
    // 3 -> 2 columns must split as {0} and {1,2}; an off-by-one would mix white into the black block or vice versa.
    const stamp = convert(
      image([
        [255, 255, 255, 255],
        [0, 0, 0, 255],
        [0, 0, 0, 255],
      ]),
      3,
      1,
      { width: 2, height: 1 },
    )
    expect(stamp.cells).toEqual([0, 28])
  })

  it('averages each block across rows as well as columns', () => {
    // A 2x2 -> 1x1 shrink must see all four pixels, not just the first row.
    const stamp = convert(
      image([
        [255, 255, 255, 0],
        [255, 255, 255, 0],
        [0, 0, 0, 255],
        [0, 0, 0, 255],
      ]),
      2,
      2,
      { width: 1, height: 1 },
    )
    expect(stamp.cells).toEqual([28])
  })

  it('refuses a byte count that does not match the size', () => {
    // A wrong stride would silently read pixels from the wrong rows.
    expect(() => convert(new Uint8ClampedArray(4), 2, 1, { width: 1, height: 1 })).toThrow()
  })

  it('refuses to grow an image', () => {
    // Growing would leave blocks with no source pixel.
    expect(() => convert(image([[0, 0, 0, 255]]), 1, 1, { width: 2, height: 1 })).toThrow()
  })
})

describe('stampPixels', () => {
  it('centres the stamp on the point with floor rounding', () => {
    // A 4x4 stamp at (10,10) must cover 8..11 on both axes.
    const stamp = { width: 4, height: 4, cells: new Array<number>(16).fill(3) }
    const pixels = stampPixels(stamp, { x: 10, y: 10 })
    expect(pixels).toHaveLength(16)
    expect(Math.min(...pixels.map((p) => p.x))).toBe(8)
    expect(Math.max(...pixels.map((p) => p.x))).toBe(11)
    expect(Math.min(...pixels.map((p) => p.y))).toBe(8)
    expect(Math.max(...pixels.map((p) => p.y))).toBe(11)
  })

  it('places cells row by row and drops skipped ones', () => {
    // Skipped cells must not become pending pixels, and the row stride must be the stamp width.
    const stamp = { width: 3, height: 2, cells: [1, undefined, 2, undefined, 4, undefined] }
    expect(stampPixels(stamp, { x: 1, y: 1 })).toEqual([
      { x: 0, y: 0, color: 1 },
      { x: 2, y: 0, color: 2 },
      { x: 1, y: 1, color: 4 },
    ])
  })
})
