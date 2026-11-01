// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { MAX_PENDING } from './geometry'
import type { Pixel } from './pack'
import { PALETTE } from './palette'
import type { Point } from './tools'

export interface Size {
  width: number
  height: number
}

export interface Stamp {
  width: number
  height: number
  cells: (number | undefined)[]
}

export const PRE_SHRINK_FACTOR = 4

export function fitSize(w: number, h: number): Size {
  if (w * h <= MAX_PENDING) return { width: w, height: h }
  const scale = Math.sqrt(MAX_PENDING / (w * h))
  let width = Math.max(1, Math.floor(w * scale + 1e-9))
  let height = Math.max(1, Math.floor(h * scale + 1e-9))
  if (width * height > MAX_PENDING) {
    if (width >= height) width = Math.floor(MAX_PENDING / height)
    else height = Math.floor(MAX_PENDING / width)
  }
  return { width, height }
}

export function preShrinkSize(w: number, h: number): Size | undefined {
  const fit = fitSize(w, h)
  const width = fit.width * PRE_SHRINK_FACTOR
  const height = fit.height * PRE_SHRINK_FACTOR
  if (w <= width && h <= height) return undefined
  return { width: Math.min(w, width), height: Math.min(h, height) }
}

export function convert(rgba: Uint8ClampedArray, w: number, h: number, size: Size): Stamp {
  if (rgba.length !== w * h * 4) throw new Error(`image has ${rgba.length} bytes, expected ${w * h * 4}`)
  if (size.width > w || size.height > h) throw new Error(`cannot grow ${w}x${h} to ${size.width}x${size.height}`)
  const cells: (number | undefined)[] = []
  for (let oy = 0; oy < size.height; oy++) {
    const y0 = Math.floor((oy * h) / size.height)
    const y1 = Math.floor(((oy + 1) * h) / size.height)
    for (let ox = 0; ox < size.width; ox++) {
      const x0 = Math.floor((ox * w) / size.width)
      const x1 = Math.floor(((ox + 1) * w) / size.width)
      let alpha = 0
      let red = 0
      let green = 0
      let blue = 0
      for (let y = y0; y < y1; y++) {
        for (let x = x0; x < x1; x++) {
          const i = (y * w + x) * 4
          const a = rgba[i + 3]
          alpha += a
          red += rgba[i] * a
          green += rgba[i + 1] * a
          blue += rgba[i + 2] * a
        }
      }
      const count = (x1 - x0) * (y1 - y0)
      cells.push(2 * alpha < 255 * count ? undefined : nearest(red / alpha, green / alpha, blue / alpha))
    }
  }
  return { width: size.width, height: size.height, cells }
}

export function stampPixels(stamp: Stamp, centre: Point): Pixel[] {
  const left = centre.x - Math.floor(stamp.width / 2)
  const top = centre.y - Math.floor(stamp.height / 2)
  const pixels: Pixel[] = []
  stamp.cells.forEach((color, i) => {
    if (color === undefined) return
    pixels.push({ x: left + (i % stamp.width), y: top + Math.floor(i / stamp.width), color })
  })
  return pixels
}

function nearest(r: number, g: number, b: number): number {
  let best = 0
  let bestDistance = Infinity
  PALETTE.forEach((rgb, index) => {
    const pr = (rgb >> 16) & 0xff
    const pg = (rgb >> 8) & 0xff
    const pb = rgb & 0xff
    const mean = (r + pr) / 2
    const distance =
      (2 + mean / 256) * (r - pr) ** 2 + 4 * (g - pg) ** 2 + (2 + (255 - mean) / 256) * (b - pb) ** 2
    if (distance < bestDistance) {
      best = index
      bestDistance = distance
    }
  })
  return best
}
