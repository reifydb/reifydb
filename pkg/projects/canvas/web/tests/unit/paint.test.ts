// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { readFileSync } from 'node:fs'
import path from 'node:path'
import { describe, expect, it } from 'vitest'
import { PALETTE, dither } from '../../public/paint.mjs'

function backendPalette(): number[] {
  const source = readFileSync(path.resolve(__dirname, '../../../backend/src/lib.rs'), 'utf8')
  const block = source.match(/pub const PALETTE: \[u32; COLORS as usize\] = \[([^\]]*)\]/)
  if (!block) throw new Error('PALETTE not found in backend/src/lib.rs')
  return [...block[1].matchAll(/0x([0-9a-f]{6})/g)].map((m) => parseInt(m[1], 16))
}

function rgb(color: number): [number, number, number] {
  return [color >> 16, (color >> 8) & 0xff, color & 0xff]
}

function patch(r: number, g: number, b: number): number[] {
  const out: number[] = []
  for (let y = 0; y < 8; y++) for (let x = 0; x < 8; x++) out.push(dither(r, g, b, x, y))
  return out
}

describe('paint helper', () => {
  it('carries exactly the backend palette, index for index', () => {
    // A stale copy paints every agent's pixels in the wrong colour with no error anywhere.
    const backend = backendPalette()
    expect(backend.length).toBe(64)
    expect([...PALETTE]).toEqual(backend)
  })

  it('keeps an exact palette colour unmixed at every pattern position', () => {
    // A flat area in a palette colour must stay flat, otherwise it turns speckled.
    PALETTE.forEach((color, index) => {
      const [r, g, b] = rgb(color)
      expect(new Set(patch(r, g, b)), `index ${index}`).toEqual(new Set([index]))
    })
  })

  it('never builds a yellow-green out of two clashing hues', () => {
    // Gold plus emerald averages to yellow-green but reads as two-colour speckle up close.
    const used = new Set(patch(0x94, 0xac, 0x34))
    expect(used.has(50) && used.has(8), `used ${[...used].map((i) => PALETTE[i].toString(16))}`).toBe(false)
  })

  it('averages a dithered patch back to the requested colour', () => {
    // Off-palette colours must land near the target, otherwise scenes shift hue.
    for (const target of [[0x16, 0x22, 0x4a], [0x6c, 0xc0, 0x40], [0xd0, 0x80, 0x50], [0x80, 0x80, 0x84]]) {
      const cells = patch(target[0], target[1], target[2]).map((i) => rgb(PALETTE[i]))
      const mean = [0, 1, 2].map((k) => cells.reduce((s, c) => s + c[k], 0) / cells.length)
      mean.forEach((v, k) => expect(Math.abs(v - target[k]), `target ${target} channel ${k}`).toBeLessThan(24))
    }
  })
})
