// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { TILE_SIZE, type TileKey, inCanvas, tileKey, tileOf } from './geometry'

export const TILE_BYTES = TILE_SIZE * TILE_SIZE

export interface ByteTransform {
  readonly writable: WritableStream<BufferSource>
  readonly readable: ReadableStream<Uint8Array>
}

export async function transform(input: Uint8Array, through: ByteTransform): Promise<Uint8Array> {
  const source = new ReadableStream<BufferSource>({
    start(controller) {
      controller.enqueue(new Uint8Array(input))
      controller.close()
    },
  })
  const reader = source.pipeThrough(through).getReader()
  const chunks: Uint8Array[] = []
  for (let read = await reader.read(); !read.done; read = await reader.read()) chunks.push(read.value)
  const output = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.length, 0))
  let offset = 0
  for (const chunk of chunks) {
    output.set(chunk, offset)
    offset += chunk.length
  }
  return output
}

export async function decodeTile(blob: Uint8Array): Promise<Uint8Array> {
  const pixels = await transform(blob, new DecompressionStream('deflate-raw'))
  if (pixels.length !== TILE_BYTES) throw new Error(`tile decodes to ${pixels.length} bytes, expected ${TILE_BYTES}`)
  return pixels
}

export interface Tile {
  readonly tx: number
  readonly ty: number
  readonly bytes: Uint8Array
  readonly version: bigint
}

export class PixelBuffer {
  private readonly tiles = new Map<TileKey, Tile>()
  private readonly dirty = new Set<TileKey>()
  private readonly listeners = new Set<() => void>()

  put(tx: number, ty: number, bytes: Uint8Array, version: bigint): boolean {
    if (bytes.length !== TILE_BYTES) throw new Error(`tile (${tx},${ty}) has ${bytes.length} bytes, expected ${TILE_BYTES}`)
    const key = tileKey(tx, ty)
    const held = this.tiles.get(key)
    if (held && held.version >= version) return false
    this.tiles.set(key, { tx, ty, bytes, version })
    this.mark(key)
    return true
  }

  drop(tx: number, ty: number): void {
    const key = tileKey(tx, ty)
    if (this.tiles.delete(key)) this.mark(key)
  }

  tile(key: TileKey): Tile | undefined {
    return this.tiles.get(key)
  }

  color(x: number, y: number): number | undefined {
    if (!inCanvas(x, y)) return undefined
    const [tx, ty] = tileOf(x, y)
    const tile = this.tiles.get(tileKey(tx, ty))
    return tile?.bytes[(y % TILE_SIZE) * TILE_SIZE + (x % TILE_SIZE)]
  }

  takeDirty(): TileKey[] {
    const dirty = [...this.dirty]
    this.dirty.clear()
    return dirty
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private mark(key: TileKey): void {
    const wasClean = this.dirty.size === 0
    this.dirty.add(key)
    if (wasClean) for (const listener of this.listeners) listener()
  }
}
