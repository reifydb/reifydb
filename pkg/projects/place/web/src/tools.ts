// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export interface Point {
  x: number
  y: number
}

export function linePixels(from: Point, to: Point): Point[] {
  const points: Point[] = []
  const dx = Math.abs(to.x - from.x)
  const dy = -Math.abs(to.y - from.y)
  const sx = from.x < to.x ? 1 : -1
  const sy = from.y < to.y ? 1 : -1
  let { x, y } = from
  let err = dx + dy
  for (;;) {
    points.push({ x, y })
    if (x === to.x && y === to.y) return points
    const e2 = 2 * err
    if (e2 >= dy) {
      err += dy
      x += sx
    }
    if (e2 <= dx) {
      err += dx
      y += sy
    }
  }
}

export function rectPixels(from: Point, to: Point): Point[] {
  const left = Math.min(from.x, to.x)
  const right = Math.max(from.x, to.x)
  const top = Math.min(from.y, to.y)
  const bottom = Math.max(from.y, to.y)
  const points = new PointSet()
  for (let x = left; x <= right; x++) {
    points.add(x, top)
    points.add(x, bottom)
  }
  for (let y = top; y <= bottom; y++) {
    points.add(left, y)
    points.add(right, y)
  }
  return points.values()
}

export function ellipsePixels(from: Point, to: Point): Point[] {
  if (from.x === to.x || from.y === to.y) return linePixels(from, to)
  let x0 = Math.min(from.x, to.x)
  let x1 = Math.max(from.x, to.x)
  const a = x1 - x0
  const b = Math.abs(to.y - from.y)
  const b1 = b & 1
  let dx = 4 * (1 - a) * b * b
  let dy = 4 * (b1 + 1) * a * a
  let err = dx + dy + b1 * a * a
  let y0 = Math.min(from.y, to.y) + Math.floor((b + 1) / 2)
  let y1 = y0 - b1
  const stepY = 8 * a * a
  const stepX = 8 * b * b
  const points = new PointSet()
  do {
    points.add(x1, y0)
    points.add(x0, y0)
    points.add(x0, y1)
    points.add(x1, y1)
    const e2 = 2 * err
    if (e2 <= dy) {
      y0++
      y1--
      dy += stepY
      err += dy
    }
    if (e2 >= dx || 2 * err > dy) {
      x0++
      x1--
      dx += stepX
      err += dx
    }
  } while (x0 <= x1)
  while (y0 - y1 < b) {
    points.add(x0 - 1, y0)
    points.add(x1 + 1, y0++)
    points.add(x0 - 1, y1)
    points.add(x1 + 1, y1--)
  }
  return points.values()
}

class PointSet {
  private readonly points = new Map<string, Point>()

  add(x: number, y: number): void {
    this.points.set(`${x},${y}`, { x, y })
  }

  values(): Point[] {
    return [...this.points.values()]
  }
}

export type Tool = 'pan' | 'pencil' | 'line' | 'rect' | 'ellipse' | 'eraser' | 'eyedropper'

export type ShapeTool = 'line' | 'rect' | 'ellipse'

export const SHAPES: Record<ShapeTool, (from: Point, to: Point) => Point[]> = {
  line: linePixels,
  rect: rectPixels,
  ellipse: ellipsePixels,
}

export function isShape(tool: Tool): tool is ShapeTool {
  return tool in SHAPES
}
