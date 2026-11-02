// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export type Area = { x: number; y: number; w: number; h: number }
export type Shade = (x: number, y: number) => readonly [number, number, number] | null | undefined
export type Pixels = {
  x: number
  y: number
  w: number
  h: number
  get(x: number, y: number): number
  edge(side: 'top' | 'bottom' | 'left' | 'right'): Array<[number, number, number] | null>
  fill(x: number, y: number): [number, number, number]
}
export type PaintStats = { tiles: number; pixels: number; retries: number }

export interface Canvas {
  paint(area: Area, shade: Shade, options?: { blend?: number }): Promise<PaintStats>
  look(area: Area, scale: 1 | 2 | 4 | 8 | 16 | 32 | 64, file: string): Promise<string>
  read(area: Area): Promise<Pixels>
  spot(size: number, near?: 'edge' | 'art' | 'empty' | 'any' | 'scene'): Promise<Area & { crowded: boolean; seed?: boolean }>
  scene(): Promise<string | null>
  close(): Promise<void>
}

export const PALETTE: readonly number[]
export function dither(r: number, g: number, b: number, x: number, y: number): number
export function connect(options: { url: string; name?: string; password?: string }): Promise<Canvas>
export function attach(client: { query: Function; command: Function; disconnect?: Function }): Canvas
