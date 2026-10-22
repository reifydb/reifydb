// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { MAX_PENDING, inCanvas } from './geometry'
import type { Pixel } from './pack'

export class Pending {
  private readonly pixels = new Map<string, Pixel>()
  private readonly listeners = new Set<() => void>()
  private version = 0

  get size(): number {
    return this.pixels.size
  }

  snapshot(): number {
    return this.version
  }

  entries(): Pixel[] {
    return [...this.pixels.values()]
  }

  get(x: number, y: number): Pixel | undefined {
    return this.pixels.get(`${x},${y}`)
  }

  add(pixel: Pixel): boolean {
    if (!inCanvas(pixel.x, pixel.y)) return false
    const key = `${pixel.x},${pixel.y}`
    if (!this.pixels.has(key) && this.pixels.size >= MAX_PENDING) return false
    this.pixels.set(key, { ...pixel })
    this.changed()
    return true
  }

  settle(committed: readonly Pixel[]): void {
    for (const pixel of committed) {
      const key = `${pixel.x},${pixel.y}`
      if (this.pixels.get(key)?.color === pixel.color) this.pixels.delete(key)
    }
    this.changed()
  }

  clear(): void {
    this.pixels.clear()
    this.changed()
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private changed(): void {
    this.version++
    for (const listener of this.listeners) listener()
  }
}
