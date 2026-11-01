// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { BlobValue, ListValue } from '@reifydb/core'
import { TILE_SIZE, tileKey, tileOf } from './geometry'

export interface Pixel {
  x: number
  y: number
  color: number
}

export function tileEdit(tx: number, ty: number, pixels: readonly Pixel[]): Uint8Array {
  const edit = new Uint8Array(2 + pixels.length * 3)
  edit[0] = tx
  edit[1] = ty
  pixels.forEach(({ x, y, color }, i) => {
    const index = (y % TILE_SIZE) * TILE_SIZE + (x % TILE_SIZE)
    edit[2 + i * 3] = index & 0xff
    edit[3 + i * 3] = index >> 8
    edit[4 + i * 3] = color
  })
  return edit
}

export function tilesParam(pixels: readonly Pixel[]): ListValue {
  const tiles = new Map<string, { tx: number; ty: number; pixels: Pixel[] }>()
  for (const pixel of pixels) {
    const [tx, ty] = tileOf(pixel.x, pixel.y)
    const key = tileKey(tx, ty)
    const tile = tiles.get(key) ?? { tx, ty, pixels: [] }
    tile.pixels.push(pixel)
    tiles.set(key, tile)
  }
  return new ListValue(
    [...tiles.values()].map(({ tx, ty, pixels }) => new BlobValue(tileEdit(tx, ty, pixels))),
    'Blob',
  )
}
