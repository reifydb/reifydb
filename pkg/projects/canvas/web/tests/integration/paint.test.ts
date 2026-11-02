// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { BlobValue } from '@reifydb/core'
import { attach } from '../../public/paint.mjs'
import type { Area, Canvas } from '../../public/paint.mjs'
import { TILE_BYTES, transform } from '@/pixels'
import { loadBackend, userClient, type TestDb, type TestFactory } from '../support/backend'
import { markReady } from '../support/seed'

const PURPLE = [0x81, 0x1e, 0x9f] as const

let create: TestFactory
let db: TestDb
let canvas: Canvas

async function seedAll(db: TestDb): Promise<void> {
  const blank = await transform(new Uint8Array(TILE_BYTES), new CompressionStream('deflate-raw'))
  for (let start = 0; start < 128 * 128; start += 1024) {
    const rows = Array.from({ length: 1024 }, (_, i) => `{ tile: ${start + i}, pixels: $pixels }`).join(', ')
    await db.commandRoot(`insert canvas::tile [${rows}]`, { pixels: new BlobValue(blank) }, [])
  }
}

function overlaps(a: Area, b: Area): boolean {
  return a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h
}

beforeAll(() => {
  create = loadBackend()
})

beforeEach(async () => {
  db = create()
  await seedAll(db)
  await markReady(db)
  canvas = attach(await userClient(db))
})

describe('paint helper against the canvas backend', () => {
  it('reads back exactly what it painted across a tile corner, and nothing outside the area', async () => {
    // Pixels straddle four tiles, so a wrong tile offset in paint or read moves some index.
    const area = { x: 40, y: 40, w: 50, h: 50 }
    const color = (x: number, y: number): number => (x * 7 + y * 3) % 64
    const { PALETTE } = await import('../../public/paint.mjs')
    const stats = await canvas.paint(area, (x, y) => {
      const c = PALETTE[color(x, y)]
      return [c >> 16, (c >> 8) & 0xff, c & 0xff]
    })
    expect(stats.pixels).toBe(50 * 50)
    const back = await canvas.read({ x: 39, y: 39, w: 52, h: 52 })
    for (let y = 39; y < 91; y++)
      for (let x = 39; x < 91; x++) {
        const inside = x >= 40 && x < 90 && y >= 40 && y < 90
        expect(back.get(x, y), `pixel (${x},${y})`).toBe(inside ? color(x, y) : 0)
      }
  })

  it('leaves a pixel untouched when the shade returns null', async () => {
    // Agents keep others' art by returning null, so null must never become white.
    const area = { x: 100, y: 100, w: 20, h: 20 }
    await canvas.paint(area, () => PURPLE)
    await canvas.paint(area, (x) => (x < 110 ? null : [0, 0, 0]), { blend: 0 })
    const back = await canvas.read(area)
    expect(back.get(105, 105)).toBe(19)
    expect(back.get(115, 105)).toBe(28)
  })

  it('tells the current scene through canvas::scene', async () => {
    // Agents read the shared brief through the helper, so it must come back as plain text.
    const scene = await canvas.scene()
    expect(scene).toContain('Deep space')
  })

  it('fades a new paint into existing art over the border band and paints fully inside it', async () => {
    // Without the fade every 512 px visit leaves a hard square seam against its neighbours.
    await canvas.paint({ x: 0, y: 0, w: 512, h: 512 }, () => PURPLE)
    await canvas.paint({ x: 128, y: 128, w: 256, h: 256 }, () => [0, 0, 0], { blend: 32 })
    const back = await canvas.read({ x: 128, y: 128, w: 256, h: 256 })
    expect(back.get(128, 256), 'the outermost pixel keeps the existing color').toBe(19)
    expect(back.get(128 + 40, 256), 'past the band the new color is exact').toBe(28)
    const inner = Array.from({ length: 5 }, (_, i) => back.get(129 + i, 256))
    expect(inner.some((c) => c !== 28), `the band is a mix, got ${inner}`).toBe(true)
  })

  it('frays a blended paint into white instead of a straight line, and paints the centre fully', async () => {
    // A straight cut against white reads as a pasted square; the border must look painted.
    const area = { x: 1024, y: 1024, w: 512, h: 512 }
    await canvas.paint(area, () => [0, 0, 0], { blend: 64 })
    const back = await canvas.read(area)
    expect(back.get(1280, 1280), 'the centre is painted').toBe(28)
    const groups: number[] = []
    for (let y0 = area.y + 128; y0 < area.y + area.h - 128; y0 += 8) {
      let white = 0
      for (let y = y0; y < y0 + 8; y++) for (let x = area.x; x < area.x + 64; x++) if (back.get(x, y) === 0) white++
      groups.push(white / 8)
    }
    expect(Math.max(...groups), 'some rows stay white near the border').toBeGreaterThan(0)
    const mean = groups.reduce((a, b) => a + b, 0) / groups.length
    const dev = groups.map((g) => g - mean)
    const lag = dev.slice(1).reduce((a, d, i) => a + d * dev[i], 0) / dev.reduce((a, d) => a + d * d, 0)
    expect(lag, `the border width changes gradually along the side, got ${groups}`).toBeGreaterThan(0.5)
  })

  it('fills an area by carrying the colors at its borders inward', async () => {
    // Agents use fill as their background, so it must continue the neighbours, not invent a new sky.
    await canvas.paint({ x: 2048, y: 2048, w: 512, h: 512 }, () => PURPLE)
    const ring = await canvas.read({ x: 2148, y: 2148, w: 300, h: 300 })
    for (const [x, y] of [[2150, 2150], [2300, 2300], [2440, 2200]]) {
      ring.fill(x, y).forEach((v, k) => expect(Math.abs(v - PURPLE[k]), `fill (${x},${y}) channel ${k}`).toBeLessThan(16))
    }
    await canvas.paint({ x: 3072, y: 3072, w: 512, h: 512 }, (x) => {
      const t = (x - 3072) / 511
      return [0x81 * (1 - t), 0x1e * (1 - t), 0x9f * (1 - t)]
    })
    const ramp = await canvas.read({ x: 3172, y: 3172, w: 312, h: 312 })
    const mid = ramp.fill(3328, 3328)
    const left = ramp.fill(3172, 3328)
    expect(mid[2], 'the centre sits between the purple left and the dark right').toBeLessThan(left[2])
    expect(mid[2], 'the centre is not black').toBeGreaterThan(10)
  })

  it('paints hard edges when blending is turned off', async () => {
    // An agent that wants a crisp shape must be able to opt out of the fade.
    await canvas.paint({ x: 0, y: 0, w: 512, h: 512 }, () => PURPLE)
    await canvas.paint({ x: 128, y: 128, w: 256, h: 256 }, () => [0, 0, 0], { blend: 0 })
    expect((await canvas.read({ x: 128, y: 128, w: 1, h: 1 })).get(128, 128)).toBe(28)
  })

  it('gives the pixels just outside each side of the area it read', async () => {
    // Agents continue what crosses into their area, so they need the neighbour's border pixels.
    await canvas.paint({ x: 99, y: 100, w: 1, h: 50 }, () => PURPLE, { blend: 0 })
    const back = await canvas.read({ x: 100, y: 100, w: 50, h: 50 })
    expect(back.edge('left')).toEqual(Array.from({ length: 50 }, () => [0x81, 0x1e, 0x9f]))
    expect(back.edge('right')).toEqual(Array.from({ length: 50 }, () => [0xff, 0xff, 0xff]))
    expect((await canvas.read({ x: 0, y: 0, w: 10, h: 10 })).edge('top')).toEqual(Array.from({ length: 10 }, () => null))
  })

  it('throws when asked for a pixel outside the area it read', async () => {
    // A silent 0 outside the area would look like white canvas and mislead rule-following agents.
    const back = await canvas.read({ x: 0, y: 0, w: 10, h: 10 })
    expect(() => back.get(10, 0)).toThrow('outside the area read')
  })

  it('finds empty spots away from art and art spots next to it', async () => {
    // A misplaced spot sends trend-starters onto others' work or reactors into blank space.
    const art = { x: 4096, y: 4096, w: 64, h: 64 }
    await canvas.paint(art, () => PURPLE)
    for (let i = 0; i < 20; i++) {
      const empty = await canvas.spot(256, 'empty')
      expect(overlaps(empty, art), `empty spot ${JSON.stringify(empty)}`).toBe(false)
      const near = await canvas.spot(128, 'art')
      const ring = { x: near.x - 16, y: near.y - 16, w: near.w + 32, h: near.h + 32 }
      expect(overlaps(ring, art), `art spot ${JSON.stringify(near)}`).toBe(true)
    }
  })

  it('sends an empty spot at least 1024 px away from recent painting', async () => {
    // Agents crowded one spot and overwrote each other; the default must push them apart.
    const art = { x: 4096, y: 4096, w: 64, h: 64 }
    await canvas.paint(art, () => PURPLE)
    for (let i = 0; i < 20; i++) {
      const s = await canvas.spot(256, 'empty')
      const dx = Math.max(art.x - (s.x + s.w), s.x - (art.x + art.w), 0)
      const dy = Math.max(art.y - (s.y + s.h), s.y - (art.y + art.h), 0)
      expect(Math.hypot(dx, dy), `spot ${JSON.stringify(s)}`).toBeGreaterThanOrEqual(1024)
      expect(s.crowded).toBe(false)
    }
  })

  it('reports an art spot as crowded when every piece of art is fresh', async () => {
    // Falling back silently would let an agent think it found a quiet neighbour.
    await canvas.paint({ x: 4096, y: 4096, w: 64, h: 64 }, () => PURPLE)
    expect((await canvas.spot(128, 'art')).crowded).toBe(true)
  })

  it('puts a scene spot only where nobody painted in the last 500 commits', async () => {
    // Scene contributors must spread over the picture instead of piling onto the same fresh part.
    const fresh = { x: 0, y: 0, w: 1536, h: 1280 }
    await canvas.paint(fresh, (x, y) => (x % 64 === 0 && y % 64 === 0 ? [0, 0, 0] : null))
    for (let i = 0; i < 40; i++) {
      const s = await canvas.spot(2048, 'scene')
      expect(overlaps(s, fresh), `scene spot ${JSON.stringify(s)}`).toBe(false)
      expect(s.crowded).toBe(false)
    }
  })

  it('seeds the picture in the middle when the wall is empty', async () => {
    // The first co-op agent has no edge to extend, so it must be told to start the picture.
    const s = await canvas.spot(512)
    expect(s).toEqual({ x: 3840, y: 3840, w: 512, h: 512, crowded: false, seed: true })
  })

  it('puts a default spot on the edge of older painting, part painted and part white', async () => {
    // Extending only works where there is something to continue and room to continue it into.
    const old = { x: 2048, y: 2048, w: 1024, h: 1024 }
    await canvas.paint(old, () => PURPLE)
    await canvas.paint({ x: 0, y: 6400, w: 8192, h: 320 }, (x, y) => (x % 64 === 0 && y % 64 === 0 ? [0, 0, 0] : null))
    for (let i = 0; i < 20; i++) {
      const s = await canvas.spot(512)
      const ix = Math.max(0, Math.min(s.x + s.w, old.x + old.w) - Math.max(s.x, old.x))
      const iy = Math.max(0, Math.min(s.y + s.h, old.y + old.h) - Math.max(s.y, old.y))
      const share = (ix * iy) / (s.w * s.h)
      expect(share, `edge spot ${JSON.stringify(s)}`).toBeGreaterThanOrEqual(0.2)
      expect(share, `edge spot ${JSON.stringify(s)}`).toBeLessThanOrEqual(0.7)
      expect(s.crowded).toBe(false)
      expect(s.seed).toBe(false)
    }
  })

  it('reports an edge spot as crowded when the only edges are fresh', async () => {
    // Extending fresh work means painting next to someone who may still be working there.
    await canvas.paint({ x: 2048, y: 2048, w: 1024, h: 1024 }, () => PURPLE)
    const s = await canvas.spot(512)
    expect(s.crowded).toBe(true)
    expect(s.seed).toBe(false)
  })

  it('prefers art nobody touched in the last 500 commits', async () => {
    // Old art is a quiet neighbour; fresh art means someone may still be working there.
    const old = { x: 1024, y: 1024, w: 64, h: 64 }
    await canvas.paint(old, () => PURPLE)
    await canvas.paint({ x: 0, y: 6400, w: 8192, h: 320 }, (x, y) => (x % 64 === 0 && y % 64 === 0 ? [0, 0, 0] : null))
    for (let i = 0; i < 10; i++) {
      const s = await canvas.spot(128, 'art')
      const ring = { x: s.x - 16, y: s.y - 16, w: s.w + 32, h: s.h + 32 }
      expect(overlaps(ring, old), `art spot ${JSON.stringify(s)}`).toBe(true)
      expect(s.crowded).toBe(false)
    }
  })
})
