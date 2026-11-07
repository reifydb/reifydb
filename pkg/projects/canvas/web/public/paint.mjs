// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { writeFileSync } from 'node:fs'
import { inflateRawSync, inflateSync } from 'node:zlib'
import { Client } from '@reifydb/client'
import { BlobValue, ListValue, Uint2Value } from '@reifydb/core'

export const PALETTE = [
  0xffffff, 0x6d001a, 0xbe0039, 0xff4500, 0xffa800, 0xffd635, 0xfff8b8, 0x00a368, 0x00cc78, 0x7eed56, 0x00756f,
  0x009eaa, 0x00ccc0, 0x2450a4, 0x3690ea, 0x51e9f4, 0x493ac1, 0x6a5cff, 0x94b3ff, 0x811e9f, 0xb44ac0, 0xe4abff,
  0xde107f, 0xff3881, 0xff99aa, 0x6d482f, 0x9c6926, 0xffb470, 0x000000, 0x515252, 0x898d90, 0xd4d7d9, 0xffdba1,
  0xeec5a6, 0xd99a6c, 0xb3774e, 0x8c4f2e, 0x5a3220, 0x152c5e, 0x004d2c, 0x4a0f1d, 0x003c41, 0x30175a, 0x3e230a,
  0x454a16, 0x4a1441, 0x7a7f2a, 0x8fae8b, 0x688cb1, 0xc25b36, 0xc9a227, 0x4f6276, 0x9c6b8a, 0xc8b98a, 0xbff0cf,
  0xb1e7ff, 0xdfd0ff, 0xffd0da, 0xffe9d6, 0xc8fefd, 0x141516, 0xb0b3b5, 0x6d7072, 0x272b2f,
]

const SIZE = 8192
const TILE = 64
const TILES = 128
const IN_FLIGHT = 8
const MAX_ATTEMPTS = 12
const BLOCK = 16
const CELL_TRIES = 4000
const CELL_W = 96
const CELL_W_SPREAD = 672
const CELL_H = 64
const CELL_H_SPREAD = 448
const CELL_RATIO_MIN = 1 / 3
const CELL_RATIO_MAX = 4
const NEIGHBOURS = [[-1, 0], [1, 0], [0, -1], [0, 1]]
const FRESH_MS = 60_000
const BAND_MIN = 0.2
const FILL_NOISE = 24

const BAYER = (() => {
  let m = [[0]]
  while (m.length < 8) {
    const n = m.length
    const next = Array.from({ length: n * 2 }, () => new Array(n * 2))
    for (let y = 0; y < n; y++)
      for (let x = 0; x < n; x++) {
        const v = m[y][x] * 4
        next[y][x] = v
        next[y][x + n] = v + 2
        next[y + n][x] = v + 3
        next[y + n][x + n] = v + 1
      }
    m = next
  }
  return m.map((row) => row.map((v) => (v + 0.5) / 64))
})()

const jitter = (x, y) => {
  let h = (x * 374761393 + y * 668265263 + 0x9e3779b9) | 0
  h = Math.imul(h ^ (h >>> 13), 1274126177)
  return ((h ^ (h >>> 16)) >>> 0) / 4294967296 - 0.5
}

const wobble = (t, seed) => {
  let v = 0
  for (let o = 0, f = 1 / 96, a = 0.6; o < 3; o++, f *= 2.3, a *= 0.45) {
    const p = t * f + seed * 17.3 + o * 5.1
    const i = Math.floor(p)
    const u = p - i
    const k = u * u * (3 - 2 * u)
    v += a * (jitter(i, seed + o) * (1 - k) + jitter(i + 1, seed + o) * k)
  }
  return 0.5 + v
}

const pal = PALETTE.map((c) => [c >> 16, (c >> 8) & 255, c & 255])
const hue = pal.map(([r, g, b]) => [r - g, (r + g) / 2 - b])
const first = new Uint8Array(32768)
const second = new Uint8Array(32768)
const share = new Float32Array(32768).fill(-1)

function solve(i) {
  const c = [((i >> 10) & 31) * 8 + 4, ((i >> 5) & 31) * 8 + 4, (i & 31) * 8 + 4]
  let best = Infinity
  for (let a = 0; a < pal.length; a++)
    for (let b = a; b < pal.length; b++) {
      const A = pal[a]
      const d = [pal[b][0] - A[0], pal[b][1] - A[1], pal[b][2] - A[2]]
      const len = 2 * d[0] * d[0] + 4 * d[1] * d[1] + 3 * d[2] * d[2]
      const along = 2 * (c[0] - A[0]) * d[0] + 4 * (c[1] - A[1]) * d[1] + 3 * (c[2] - A[2]) * d[2]
      const t = len ? Math.max(0, Math.min(1, along / len)) : 0
      const e = [c[0] - A[0] - d[0] * t, c[1] - A[1] - d[1] * t, c[2] - A[2] - d[2] * t]
      const [ua, va] = hue[a]
      const [ub, vb] = hue[b]
      const clash = Math.hypot(ua, va) * Math.hypot(ub, vb) - (ua * ub + va * vb)
      const cost = 2 * e[0] * e[0] + 4 * e[1] * e[1] + 3 * e[2] * e[2] + t * (1 - t) * (0.12 * len + 3 * clash)
      if (cost < best) {
        best = cost
        first[i] = a
        second[i] = b
        share[i] = t
      }
    }
}

const exact = new Map(PALETTE.map((c, i) => [c, i]))

export function dither(r, g, b, x, y) {
  const hit = exact.get((r << 16) | (g << 8) | b)
  if (hit !== undefined && Number.isInteger(r) && Number.isInteger(g) && Number.isInteger(b)) return hit
  const j = jitter(x, y) * 8
  const q = (v) => Math.max(0, Math.min(31, Math.round(v + j) >> 3))
  const i = (q(r) << 10) | (q(g) << 5) | q(b)
  if (share[i] < 0) solve(i)
  return BAYER[y & 7][x & 7] < share[i] ? second[i] : first[i]
}

function clip(area) {
  const x0 = Math.max(0, Math.floor(area.x))
  const y0 = Math.max(0, Math.floor(area.y))
  const x1 = Math.min(SIZE, Math.floor(area.x + area.w))
  const y1 = Math.min(SIZE, Math.floor(area.y + area.h))
  if (x1 <= x0 || y1 <= y0) throw new Error(`area ${JSON.stringify(area)} does not overlap the 8192 x 8192 canvas`)
  return { x0, y0, x1, y1 }
}

function tilesOf({ x0, y0, x1, y1 }) {
  const tiles = []
  for (let ty = Math.floor(y0 / TILE); ty <= Math.floor((y1 - 1) / TILE); ty++)
    for (let tx = Math.floor(x0 / TILE); tx <= Math.floor((x1 - 1) / TILE); tx++) tiles.push([tx, ty])
  return tiles
}

function edit(tx, ty, box, shade) {
  const bytes = new Uint8Array(2 + TILE * TILE * 3)
  bytes[0] = tx
  bytes[1] = ty
  let at = 2
  for (let y = Math.max(box.y0, ty * TILE); y < Math.min(box.y1, ty * TILE + TILE); y++)
    for (let x = Math.max(box.x0, tx * TILE); x < Math.min(box.x1, tx * TILE + TILE); x++) {
      const rgb = shade(x, y)
      if (rgb == null) continue
      const index = (y % TILE) * TILE + (x % TILE)
      bytes[at++] = index & 0xff
      bytes[at++] = index >> 8
      bytes[at++] = dither(rgb[0], rgb[1], rgb[2], x, y)
    }
  return at === 2 ? null : bytes.subarray(0, at)
}

const retryable = (e) => e?.code === 'TXN_001' || String(e?.message ?? e).includes('canvas not ready')

async function commit(client, bytes, tx, ty) {
  const tiles = new ListValue([new BlobValue(bytes)], 'Blob')
  for (let attempt = 1, wait = 50; ; attempt++, wait *= 2) {
    try {
      await client.command('CALL canvas::commit($tiles)', { tiles }, [])
      return attempt - 1
    } catch (e) {
      if (!retryable(e) || attempt >= MAX_ATTEMPTS)
        throw new Error(`tile (${tx},${ty}) failed after ${attempt} attempts: ${e?.code ?? ''} ${e?.message ?? e}`)
      await new Promise((resolve) => setTimeout(resolve, Math.min(wait, 5000) * (1 + Math.random())))
    }
  }
}

async function paint(client, area, shade, options = {}) {
  const box = clip(area)
  const band = options.blend ?? 0
  if (!Number.isInteger(band) || band < 0) throw new Error(`blend ${band} must be a whole number of pixels, 0 or more`)
  if (band > 0) {
    const at = await pixels(client, box)
    const own = shade
    shade = (x, y) => {
      const rgb = own(x, y)
      if (rgb == null) return rgb
      const sides = [
        [x - box.x0, y, 1],
        [box.x1 - 1 - x, y, 2],
        [y - box.y0, x, 3],
        [box.y1 - 1 - y, x, 4],
      ]
      let t = 1
      for (const [d, along, seed] of sides) {
        const width = band * (BAND_MIN + (1 - BAND_MIN) * Math.max(0, Math.min(1, wobble(along, seed))))
        t = Math.min(t, d / width)
      }
      if (t >= 1) return rgb
      const old = at(x, y)
      if (old === 0) return jitter(x + 7919, y) + 0.5 < t * t * (3 - 2 * t) ? rgb : null
      const was = rgbOf(old)
      return [was[0] + (rgb[0] - was[0]) * t, was[1] + (rgb[1] - was[1]) * t, was[2] + (rgb[2] - was[2]) * t]
    }
  }
  const queue = tilesOf(box)
  const stats = { tiles: 0, pixels: 0, retries: 0 }
  await Promise.all(
    Array.from({ length: IN_FLIGHT }, async () => {
      while (queue.length) {
        const [tx, ty] = queue.shift()
        const bytes = edit(tx, ty, box, shade)
        if (!bytes) continue
        stats.retries += await commit(client, bytes, tx, ty)
        stats.tiles++
        stats.pixels += (bytes.length - 2) / 3
      }
    }),
  )
  return stats
}

const keysOf = (box) => new ListValue(tilesOf(box).map(([tx, ty]) => new Uint2Value(ty * TILES + tx)), 'Uint2')

async function png(client, box, scale) {
  const frames = await client.query(
    `from canvas::tile filter { tile in $keys } aggregate { png: canvas::png(canvas::pack(tile, pixels), ${scale}) }`,
    { keys: keysOf(box) },
    [],
  )
  return Buffer.from(frames[0][0].png.value)
}

async function look(client, area, scale, file) {
  writeFileSync(file, await png(client, clip(area), scale))
  return file
}

async function pixels(client, box) {
  const w = box.x1 - box.x0
  const data = new Uint8Array(w * (box.y1 - box.y0))
  const frames = await client.query('from canvas::tile filter { tile in $keys } map { tile, pixels }', { keys: keysOf(box) }, [])
  const seen = new Set()
  for (const row of frames[0]) {
    const key = Number(row.tile.value)
    const tx = key % TILES
    const ty = Math.floor(key / TILES)
    const bytes = inflateRawSync(Buffer.from(row.pixels.value))
    if (bytes.length !== TILE * TILE) throw new Error(`tile (${tx},${ty}) inflated to ${bytes.length} bytes, expected ${TILE * TILE}`)
    seen.add(key)
    for (let y = Math.max(box.y0, ty * TILE); y < Math.min(box.y1, ty * TILE + TILE); y++)
      for (let x = Math.max(box.x0, tx * TILE); x < Math.min(box.x1, tx * TILE + TILE); x++)
        data[(y - box.y0) * w + (x - box.x0)] = bytes[(y % TILE) * TILE + (x % TILE)]
  }
  for (const [tx, ty] of tilesOf(box)) if (!seen.has(ty * TILES + tx)) throw new Error(`tile (${tx},${ty}) is missing from the canvas`)
  return (x, y) => data[(y - box.y0) * w + (x - box.x0)]
}

const rgbOf = (index) => [PALETTE[index] >> 16, (PALETTE[index] >> 8) & 255, PALETTE[index] & 255]

async function read(client, area) {
  const box = clip(area)
  const outer = { x0: Math.max(0, box.x0 - 1), y0: Math.max(0, box.y0 - 1), x1: Math.min(SIZE, box.x1 + 1), y1: Math.min(SIZE, box.y1 + 1) }
  const at = await pixels(client, outer)
  const outside = (x, y) => (x < 0 || y < 0 || x >= SIZE || y >= SIZE ? null : rgbOf(at(x, y)))
  const sides = {
    top: () => Array.from({ length: box.x1 - box.x0 }, (_, i) => outside(box.x0 + i, box.y0 - 1)),
    bottom: () => Array.from({ length: box.x1 - box.x0 }, (_, i) => outside(box.x0 + i, box.y1)),
    left: () => Array.from({ length: box.y1 - box.y0 }, (_, i) => outside(box.x0 - 1, box.y0 + i)),
    right: () => Array.from({ length: box.y1 - box.y0 }, (_, i) => outside(box.x1, box.y0 + i)),
  }
  const ring = { top: sides.top(), bottom: sides.bottom(), left: sides.left(), right: sides.right() }
  const known = Object.values(ring).flat().filter(Boolean)
  const mean = known.length ? [0, 1, 2].map((k) => known.reduce((a, c) => a + c[k], 0) / known.length) : [255, 255, 255]
  for (const side of Object.keys(ring)) ring[side] = ring[side].map((c) => c ?? mean)
  const w = box.x1 - box.x0
  const h = box.y1 - box.y0
  const corner = (a, b) => [0, 1, 2].map((k) => (a[k] + b[k]) / 2)
  const tl = corner(ring.top[0], ring.left[0])
  const tr = corner(ring.top[w - 1], ring.right[0])
  const bl = corner(ring.bottom[0], ring.left[h - 1])
  const br = corner(ring.bottom[w - 1], ring.right[h - 1])
  return {
    x: box.x0,
    y: box.y0,
    w,
    h,
    fill(x, y) {
      if (x < box.x0 || x >= box.x1 || y < box.y0 || y >= box.y1) throw new Error(`pixel (${x},${y}) is outside the area read`)
      const i = x - box.x0
      const j = y - box.y0
      const u = w > 1 ? i / (w - 1) : 0.5
      const v = h > 1 ? j / (h - 1) : 0.5
      const n = (wobble(i + j * 0.7, 9) - 0.5) * FILL_NOISE
      return [0, 1, 2].map((k) => {
        const c =
          (1 - v) * ring.top[i][k] + v * ring.bottom[i][k] + (1 - u) * ring.left[j][k] + u * ring.right[j][k] -
          ((1 - u) * (1 - v) * tl[k] + u * (1 - v) * tr[k] + (1 - u) * v * bl[k] + u * v * br[k])
        return Math.max(0, Math.min(255, c + n))
      })
    },
    get(x, y) {
      if (x < box.x0 || x >= box.x1 || y < box.y0 || y >= box.y1) throw new Error(`pixel (${x},${y}) is outside the area read`)
      return at(x, y)
    },
    edge(side) {
      if (!(side in sides)) throw new Error(`edge side '${side}' must be 'top', 'bottom', 'left' or 'right'`)
      return sides[side]()
    },
  }
}

function decodeBlocks(bytes) {
  let at = 8
  let width = 0
  let height = 0
  const idat = []
  while (at < bytes.length) {
    const len = bytes.readUInt32BE(at)
    const kind = bytes.toString('latin1', at + 4, at + 8)
    if (kind === 'IHDR') {
      width = bytes.readUInt32BE(at + 8)
      height = bytes.readUInt32BE(at + 12)
    }
    if (kind === 'IDAT') idat.push(bytes.subarray(at + 8, at + 8 + len))
    at += 12 + len
  }
  const raw = inflateSync(Buffer.concat(idat))
  const blocks = new Uint8Array(width * height)
  for (let y = 0; y < height; y++) {
    const line = y * (width + 1)
    if (raw[line] !== 0) throw new Error(`canvas png row ${y} uses filter ${raw[line]}, expected 0`)
    blocks.set(raw.subarray(line + 1, line + 1 + width), y * width)
  }
  return { width, blocks }
}

async function busyTiles(client) {
  const since = Date.now() - FRESH_MS
  const frames = await client.query(`from canvas::edit filter { datetime::epoch_millis(#created_at) > ${since} } map { tile }`, {}, [])
  const keys = new Set(frames[0].map((row) => Number(row.tile.value ?? row.tile)))
  return [...keys].map((key) => [key % TILES, Math.floor(key / TILES)])
}

function cellSize() {
  for (;;) {
    const w = Math.round((CELL_W + CELL_W_SPREAD * Math.random() ** 2) / BLOCK) * BLOCK
    const h = Math.round((CELL_H + CELL_H_SPREAD * Math.random() ** 2) / BLOCK) * BLOCK
    if (w / h >= CELL_RATIO_MIN && w / h <= CELL_RATIO_MAX) return { w, h }
  }
}

async function cell(client) {
  const { w, h } = cellSize()
  const grid = (v) => Math.floor(v / BLOCK) * BLOCK
  const { width, blocks } = decodeBlocks(await png(client, { x0: 0, y0: 0, x1: SIZE, y1: SIZE }, BLOCK))
  if (blocks.every((b) => b === 0)) return { x: grid((SIZE - w) / 2), y: grid((SIZE - h) / 2), w, h }
  const busy = await busyTiles(client)
  const painted = (bx, by) => bx >= 0 && by >= 0 && bx < width && by < width && blocks[by * width + bx] !== 0
  const anchors = []
  for (let by = 0; by < width; by++)
    for (let bx = 0; bx < width; bx++) {
      if (blocks[by * width + bx] !== 0) continue
      const sides = NEIGHBOURS.filter(([dx, dy]) => painted(bx + dx, by + dy))
      if (sides.length > 0) anchors.push([bx, by, sides])
    }
  const start = (b, d, n) => (d < 0 ? b : d > 0 ? b - n + 1 : b - Math.floor(Math.random() * n))
  const clamp = (v, n) => Math.max(0, Math.min(SIZE - n, v))
  const place = () => {
    if (anchors.length === 0) return { x: grid(Math.random() * (SIZE - w)), y: grid(Math.random() * (SIZE - h)), w, h }
    const [bx, by, sides] = anchors[Math.floor(Math.random() * anchors.length)]
    const [dx, dy] = sides[Math.floor(Math.random() * sides.length)]
    return { x: clamp(start(bx, dx, w / BLOCK) * BLOCK, w), y: clamp(start(by, dy, h / BLOCK) * BLOCK, h), w, h }
  }
  const quiet = (c) =>
    busy.every(([tx, ty]) => tx * TILE >= c.x + c.w || c.x >= tx * TILE + TILE || ty * TILE >= c.y + c.h || c.y >= ty * TILE + TILE)
  let last
  for (let i = 0; i < CELL_TRIES; i++) {
    last = place()
    if (quiet(last)) return last
  }
  return last
}

export async function connect({ url, name, password }) {
  const client = await Client.connectWs(url, { format: 'rbcf' })
  if (name !== undefined) await client.loginWithPassword(name, password)
  return attach(client)
}

export function attach(client) {
  return {
    paint: (area, shade, options) => paint(client, area, shade, options),
    look: (area, scale, file) => look(client, area, scale, file),
    read: (area) => read(client, area),
    cell: () => cell(client),
    close: () => client.disconnect(),
  }
}
