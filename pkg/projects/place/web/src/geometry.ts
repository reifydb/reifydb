// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export const CANVAS_SIZE = 2048
export const TILE_SIZE = 64
export const TILES_PER_SIDE = CANVAS_SIZE / TILE_SIZE
export const MAX_PENDING = 1024

export type TileKey = string

export function tileOf(x: number, y: number): [number, number] {
  return [Math.floor(x / TILE_SIZE), Math.floor(y / TILE_SIZE)]
}

export function tileKey(tx: number, ty: number): TileKey {
  return `${tx},${ty}`
}

export function parseTileKey(key: TileKey): [number, number] {
  const [tx, ty] = key.split(',').map(Number)
  return [tx, ty]
}

export function inCanvas(x: number, y: number): boolean {
  return Number.isInteger(x) && Number.isInteger(y) && x >= 0 && y >= 0 && x < CANVAS_SIZE && y < CANVAS_SIZE
}

export function tilesInRect(left: number, top: number, right: number, bottom: number): Set<TileKey> {
  const tiles = new Set<TileKey>()
  const clamp = (v: number) => Math.min(TILES_PER_SIDE - 1, Math.max(0, v))
  const tx0 = clamp(Math.floor(left / TILE_SIZE))
  const ty0 = clamp(Math.floor(top / TILE_SIZE))
  const tx1 = clamp(Math.floor((right - 1) / TILE_SIZE))
  const ty1 = clamp(Math.floor((bottom - 1) / TILE_SIZE))
  if (right <= 0 || bottom <= 0 || left >= CANVAS_SIZE || top >= CANVAS_SIZE) return tiles
  for (let tx = tx0; tx <= tx1; tx++) {
    for (let ty = ty0; ty <= ty1; ty++) tiles.add(tileKey(tx, ty))
  }
  return tiles
}
