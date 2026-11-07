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
const SIGN = { topic: 'test topic', joke: 'test joke' }

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

  it('spreads cells over all four quarters of an empty wall', async () => {
    // Starting every cell at the centre piled signs on top of each other while the rest of the wall stayed blank.
    const quarters = new Set<string>()
    for (let i = 0; i < 30; i++) {
      const c = await canvas.cell()
      quarters.add(`${c.x + c.w / 2 < 4096 ? 'w' : 'e'}${c.y + c.h / 2 < 4096 ? 'n' : 's'}`)
    }
    expect([...quarters].sort()).toEqual(['en', 'es', 'wn', 'ws'])
  }, 30000)

  it('spreads cell sizes over the whole range, favouring small, within the allowed shapes', async () => {
    // A fixed or narrow size turns the mosaic back into a grid; slivers outside 1:3..4:1 cannot hold a sign.
    await canvas.paint({ x: 2048, y: 2048, w: 1024, h: 1024 }, () => PURPLE)
    const cells = []
    for (let i = 0; i < 40; i++) cells.push(await canvas.cell())
    for (const c of cells) {
      expect(c.w % 16, `cell ${JSON.stringify(c)}`).toBe(0)
      expect(c.h % 16, `cell ${JSON.stringify(c)}`).toBe(0)
      expect(c.w).toBeGreaterThanOrEqual(192)
      expect(c.w).toBeLessThanOrEqual(768)
      expect(c.h).toBeGreaterThanOrEqual(128)
      expect(c.h).toBeLessThanOrEqual(512)
      expect(c.w / c.h).toBeGreaterThanOrEqual(1 / 3)
      expect(c.w / c.h).toBeLessThanOrEqual(4)
      expect(c.x >= 0 && c.y >= 0 && c.x + c.w <= 8192 && c.y + c.h <= 8192, `cell ${JSON.stringify(c)}`).toBe(true)
    }
    expect(new Set(cells.map((c) => `${c.w}x${c.h}`)).size).toBeGreaterThanOrEqual(15)
    expect(cells.filter((c) => c.w < 480).length).toBeGreaterThan(20)
    expect(cells.some((c) => c.w > 500)).toBe(true)
  }, 30000)

  it('puts a cell on blank wall, never on older art, while blank space is left', async () => {
    // Landing on a sign while most of the wall is blank buries it for no reason.
    const old = { x: 2048, y: 2048, w: 2048, h: 2048 }
    await canvas.paint(old, () => PURPLE)
    for (let i = 0; i < 40; i++) {
      const c = await canvas.cell()
      expect(overlaps(c, old), `cell ${JSON.stringify(c)}`).toBe(false)
    }
  }, 30000)

  it('paints an svg rect in an exact palette colour over the whole area', async () => {
    // A shifted or scaled render would leave a white rim or bleed outside the cell.
    const area = { x: 1000, y: 1000, w: 96, h: 64 }
    await canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="64" viewBox="0 0 96 64"><rect width="96" height="64" fill="#ff4500"/></svg>', SIGN)
    const back = await canvas.read({ x: 999, y: 999, w: 98, h: 66 })
    for (let y = 999; y < 1065; y++)
      for (let x = 999; x < 1097; x++) {
        const inside = x >= 1000 && x < 1096 && y >= 1000 && y < 1064
        expect(back.get(x, y), `pixel (${x},${y})`).toBe(inside ? 3 : 0)
      }
  })

  it('keeps existing pixels where the svg is transparent', async () => {
    // Treating transparency as white would wipe every sign under a cell with a rounded or odd shape.
    const area = { x: 2000, y: 2000, w: 96, h: 64 }
    await canvas.paint(area, () => PURPLE)
    await canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="64" viewBox="0 0 96 64"><rect width="48" height="64" fill="#000000"/></svg>', SIGN)
    const back = await canvas.read(area)
    expect(back.get(2010, 2030)).toBe(28)
    expect(back.get(2080, 2030)).toBe(19)
  })

  it('refuses an svg whose size does not match the area', async () => {
    // A silent rescale would stretch the sign and misplace every pixel against the cell.
    const area = { x: 3000, y: 3000, w: 96, h: 64 }
    await expect(
      canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96"><rect width="96" height="96"/></svg>', SIGN),
    ).rejects.toThrow('svg renders at 96 x 96')
  })

  it('draws svg text in a Fontsource font, only inside the area', async () => {
    // Without the font loaded resvg draws no glyphs, so a sign silently loses all its words.
    const area = { x: 4000, y: 4000, w: 192, h: 64 }
    await canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="192" height="64" viewBox="0 0 192 64"><text x="4" y="48" font-family="Bungee" font-size="40" fill="#000000">WE BUY</text></svg>', SIGN)
    const back = await canvas.read({ x: 3990, y: 3990, w: 212, h: 84 })
    let ink = 0
    for (let y = 3990; y < 4074; y++)
      for (let x = 3990; x < 4202; x++) {
        const inside = x >= 4000 && x < 4192 && y >= 4000 && y < 4064
        if (!inside) expect(back.get(x, y), `pixel (${x},${y})`).toBe(0)
        else if (back.get(x, y) !== 0) ink++
      }
    expect(ink).toBeGreaterThan(500)
  }, 30000)

  it('refuses svg text below 24 px or with no font-size at all', async () => {
    // Text under 24 px dithers into mush on the wall, so a cramped sign must fail instead of painting.
    const area = { x: 4000, y: 6000, w: 96, h: 64 }
    const text = (attrs: string) =>
      `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="64" viewBox="0 0 96 64"><text x="4" y="40" font-family="Bungee" ${attrs}>HI</text></svg>`
    await expect(canvas.svg(area, text('font-size="10"'), SIGN)).rejects.toThrow('font-size 10 below 24 px')
    await expect(canvas.svg(area, text('style="font-size: 23.5px"'), SIGN)).rejects.toThrow('font-size 23.5 below 24 px')
    await expect(canvas.svg(area, text(''), SIGN)).rejects.toThrow('svg text has no font-size')
    await expect(canvas.svg(area, text('font-size="2em"'), SIGN)).rejects.toThrow('font-size 2em must be in px')
    await canvas.svg(area, text('font-size="24"'), SIGN)
    const back = await canvas.read(area)
    let ink = 0
    for (let y = area.y; y < area.y + area.h; y++) for (let x = area.x; x < area.x + area.w; x++) if (back.get(x, y) !== 0) ink++
    expect(ink).toBeGreaterThan(0)
  }, 30000)

  it('fails loudly on a font family that Fontsource does not have', async () => {
    // A typo in a font name must not turn into a sign with no text.
    const area = { x: 4000, y: 5000, w: 96, h: 64 }
    await expect(
      canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="64" viewBox="0 0 96 64"><text font-family="Not A Real Font Xyz">HI</text></svg>', SIGN),
    ).rejects.toThrow("font 'Not A Real Font Xyz' is not on Fontsource")
  }, 30000)

  it('inlines an Iconify icon referenced as icon:prefix:name', async () => {
    // An unresolved icon reference renders nothing, so the picture half of a sign would vanish.
    const area = { x: 5000, y: 5000, w: 64, h: 64 }
    await canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64"><use href="icon:mdi:square" x="0" y="0" width="64" height="64" color="#000000"/></svg>', SIGN)
    const back = await canvas.read(area)
    expect(back.get(5032, 5032)).toBe(28)
    await expect(
      canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64"><use href="icon:mdi:not-an-icon-xyz" width="64" height="64"/></svg>', SIGN),
    ).rejects.toThrow("icon 'mdi:not-an-icon-xyz' not found on Iconify")
  }, 30000)

  it('measures text so a wider string comes out wider and an unknown font throws', async () => {
    // Without a measure, agents guessed widths and their big lines ran past the cell.
    const one = await canvas.measure('W', 'Bungee', 40)
    const three = await canvas.measure('WWW', 'Bungee', 40)
    expect(one.w).toBeGreaterThan(10)
    expect(three.w).toBeGreaterThan(one.w * 2.5)
    expect(three.w).toBeLessThan(one.w * 3.5)
    expect(one.h).toBeGreaterThan(20)
    expect(one.h).toBeLessThanOrEqual(40)
    await expect(canvas.measure('W', 'Not A Real Font Xyz', 40)).rejects.toThrow('is not on Fontsource')
  }, 30000)

  it('applies a transform on an icon reference instead of dropping it', async () => {
    // A nested svg ignores transform, so a moved, flipped or rotated icon silently stayed where it was.
    const area = { x: 5200, y: 5000, w: 128, h: 64 }
    await canvas.svg(area, '<svg xmlns="http://www.w3.org/2000/svg" width="128" height="64" viewBox="0 0 128 64"><use href="icon:mdi:square" x="0" y="0" width="64" height="64" color="#000000" transform="translate(64 0)"/></svg>', SIGN)
    const back = await canvas.read(area)
    expect(back.get(5232, 5032), 'left half stays white once the icon moved').toBe(0)
    expect(back.get(5296, 5032), 'right half holds the moved icon').toBe(28)
  }, 30000)

  it('refuses an svg without topic and joke and paints nothing', async () => {
    // A sign missing from the log lets the next agent paint the same joke again.
    const area = { x: 6000, y: 6000, w: 96, h: 64 }
    const rect = '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="64" viewBox="0 0 96 64"><rect width="96" height="64" fill="#000000"/></svg>'
    await expect(canvas.svg(area, rect, undefined as never)).rejects.toThrow('needs the topic')
    await expect(canvas.svg(area, rect, { topic: 'only topic' } as never)).rejects.toThrow('needs the topic')
    expect((await canvas.read(area)).get(6010, 6010)).toBe(0)
    expect(await canvas.signs()).toEqual([])
  })

  it('rolls varied styles with real fonts and 2 to 4 distinct colours, at least one dark', async () => {
    // A style that repeats or names no real font lets every agent fall back to the same look.
    const { PALETTE } = await import('../../public/paint.mjs')
    const styles = []
    for (let i = 0; i < 30; i++) styles.push(await canvas.style())
    for (const s of styles) {
      expect(s.era.length).toBeGreaterThan(0)
      expect(s.surface.length).toBeGreaterThan(0)
      expect(s.shape.length).toBeGreaterThan(0)
      expect(s.fonts).toHaveLength(2)
      expect(s.colors.length).toBeGreaterThanOrEqual(2)
      expect(s.colors.length).toBeLessThanOrEqual(4)
      expect(new Set(s.colors).size).toBe(s.colors.length)
      const dark = s.colors.some((i: number) => {
        const c = PALETTE[i]
        return ((c >> 16) * 299 + ((c >> 8) & 255) * 587 + (c & 255) * 114) / 1000 < 128
      })
      expect(dark, `style ${JSON.stringify(s)}`).toBe(true)
    }
    expect(new Set(styles.map((s) => `${s.era}|${s.surface}|${s.shape}`)).size).toBeGreaterThanOrEqual(10)
    expect(new Set(styles.map((s) => s.fonts[0])).size).toBeGreaterThanOrEqual(10)
  }, 30000)

  it('rolls a display font for the big line and a plain serif or sans font for small print, never an unreadable one', async () => {
    // Thin scripts and bar fonts like Redacted make the big line unreadable, so agents threw those rolls away.
    const list = await (await fetch('https://api.fontsource.org/v1/fonts')).json()
    const category = new Map(list.map((f: { family: string; category: string }) => [f.family, f.category]))
    for (let i = 0; i < 40; i++) {
      const s = await canvas.style()
      expect(category.get(s.fonts[0]), `big font ${s.fonts[0]}`).toBe('display')
      expect(['serif', 'sans-serif'], `small font ${s.fonts[1]}`).toContain(category.get(s.fonts[1]))
      for (const f of s.fonts) expect(f.startsWith('Redacted') || f.startsWith('Flow '), `font ${f}`).toBe(false)
    }
  }, 30000)
})
