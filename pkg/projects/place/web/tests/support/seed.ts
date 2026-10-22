// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { TestDb } from '@reifydb/reifydb'

export async function seedTile(db: TestDb, tx: number, ty: number): Promise<void> {
  const rows: string[] = []
  for (let dy = 0; dy < 64; dy++) {
    for (let dx = 0; dx < 64; dx++) {
      rows.push(`{ tile_x: ${tx}, tile_y: ${ty}, x: ${tx * 64 + dx}, y: ${ty * 64 + dy}, color: 0 }`)
    }
  }
  await db.commandRoot(`insert place::live [${rows.join(', ')}]`, {}, [])
}

export async function colorAt(db: TestDb, x: number, y: number): Promise<number | undefined> {
  const tx = Math.floor(x / 64)
  const ty = Math.floor(y / 64)
  const frames = (await db.queryRoot(
    `from place::live filter { tile_x == ${tx} and tile_y == ${ty} and x == ${x} and y == ${y} } map { color }`,
    {},
    [],
  )) as unknown as Array<Array<{ color: unknown }>>
  const row = frames[0]?.[0]
  if (row === undefined) return undefined
  const color = row.color as { value?: unknown }
  return Number(typeof color === 'object' && color !== null && 'value' in color ? color.value : color)
}

export async function markReady(db: TestDb): Promise<void> {
  await db.commandRoot('insert place::state [{ key: "canvas", ready: true }]', {}, [])
}
