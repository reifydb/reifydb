// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { BlobValue, Uint1Value } from '@reifydb/core'
import type { TestDb } from '@reifydb/reifydb'
import { bytes } from '@/client'
import { TILE_BYTES, decodeTile, transform } from '@/pixels'

function encodeTile(pixels: Uint8Array): Promise<Uint8Array> {
  return transform(pixels, new CompressionStream('deflate-raw'))
}

export async function seedTile(db: TestDb, tx: number, ty: number): Promise<void> {
  const blank = await encodeTile(new Uint8Array(TILE_BYTES))
  await db.commandRoot(
    'insert place::tile [{ tile_x: $tx, tile_y: $ty, pixels: $pixels }]',
    { tx: new Uint1Value(tx), ty: new Uint1Value(ty), pixels: new BlobValue(blank) },
    [],
  )
}

export async function colorAt(db: TestDb, x: number, y: number): Promise<number | undefined> {
  const tx = Math.floor(x / 64)
  const ty = Math.floor(y / 64)
  const frames = (await db.queryRoot(
    `from place::tile filter { tile_x == ${tx} and tile_y == ${ty} } map { tile_x, pixels }`,
    {},
    [],
  )) as unknown as Array<Array<{ pixels: unknown }>>
  const row = frames[0]?.[0]
  if (row === undefined) return undefined
  return (await decodeTile(bytes(row.pixels)))[(y % 64) * 64 + (x % 64)]
}

export async function markReady(db: TestDb): Promise<void> {
  await db.commandRoot('insert place::state [{ key: "canvas", ready: true }]', {}, [])
}
