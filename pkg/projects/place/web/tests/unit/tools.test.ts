// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { type Point, ellipsePixels, linePixels, rectPixels } from '@/tools'

const keys = (points: Point[]) => new Set(points.map((p) => `${p.x},${p.y}`))

function connected(points: Point[]): boolean {
  for (let i = 1; i < points.length; i++) {
    const step = Math.max(Math.abs(points[i].x - points[i - 1].x), Math.abs(points[i].y - points[i - 1].y))
    if (step !== 1) return false
  }
  return true
}

describe('linePixels', () => {
  it('includes both endpoints in order', () => {
    // A dropped endpoint leaves the pixel the user aimed at unpainted.
    const points = linePixels({ x: 3, y: 7 }, { x: 20, y: 2 })
    expect(points[0]).toEqual({ x: 3, y: 7 })
    expect(points[points.length - 1]).toEqual({ x: 20, y: 2 })
  })

  it('has no gaps and no repeats in every direction', () => {
    // Gaps show as dotted pencil strokes; repeats would waste pending budget.
    const targets = [
      { x: 10, y: 0 }, { x: -10, y: 0 }, { x: 0, y: 10 }, { x: 0, y: -10 },
      { x: 7, y: 3 }, { x: -3, y: 9 }, { x: -8, y: -8 }, { x: 5, y: -12 },
    ]
    for (const target of targets) {
      const points = linePixels({ x: 0, y: 0 }, target)
      expect(connected(points)).toBe(true)
      expect(points.length).toBe(Math.max(Math.abs(target.x), Math.abs(target.y)) + 1)
      expect(keys(points).size).toBe(points.length)
    }
  })

  it('returns one pixel for a zero length line', () => {
    // A pencil event with no movement must still paint exactly the pixel under the cursor.
    expect(linePixels({ x: 4, y: 4 }, { x: 4, y: 4 })).toEqual([{ x: 4, y: 4 }])
  })
})

describe('rectPixels', () => {
  it('draws only the outline with the exact count', () => {
    // An off by one corner either leaves a hole or double counts against the 1024 limit.
    const points = rectPixels({ x: 10, y: 20 }, { x: 14, y: 23 })
    expect(points.length).toBe(2 * 5 + 2 * 4 - 4)
    for (const p of points) expect(p.x === 10 || p.x === 14 || p.y === 20 || p.y === 23).toBe(true)
    for (const corner of ['10,20', '14,20', '10,23', '14,23']) expect(keys(points).has(corner)).toBe(true)
  })

  it('gives the same pixels whichever corner the drag starts from', () => {
    // Dragging up-left must not produce a different shape than dragging down-right.
    expect(keys(rectPixels({ x: 14, y: 23 }, { x: 10, y: 20 }))).toEqual(keys(rectPixels({ x: 10, y: 20 }, { x: 14, y: 23 })))
  })

  it('collapses to a line or a point without duplicates', () => {
    // A flat drag must cost its true pixel count, not twice that.
    expect(rectPixels({ x: 0, y: 5 }, { x: 9, y: 5 }).length).toBe(10)
    expect(rectPixels({ x: 3, y: 3 }, { x: 3, y: 3 }).length).toBe(1)
  })
})

describe('ellipsePixels', () => {
  const boxes = [
    [0, 0, 10, 6], [0, 0, 11, 7], [5, 5, 5, 12], [0, 0, 1, 1], [2, 3, 40, 40], [0, 0, 9, 0],
  ] as const

  it('stays inside the dragged box and touches all four sides', () => {
    // An ellipse that spills out or falls short of the box does not match the preview rectangle.
    for (const [x0, y0, x1, y1] of boxes) {
      const points = ellipsePixels({ x: x0, y: y0 }, { x: x1, y: y1 })
      for (const p of points) {
        expect(p.x >= x0 && p.x <= x1 && p.y >= y0 && p.y <= y1).toBe(true)
      }
      expect(points.some((p) => p.x === x0)).toBe(true)
      expect(points.some((p) => p.x === x1)).toBe(true)
      expect(points.some((p) => p.y === y0)).toBe(true)
      expect(points.some((p) => p.y === y1)).toBe(true)
    }
  })

  it('is mirror symmetric on both axes', () => {
    // Asymmetry is the usual symptom of a wrong half step in the midpoint algorithm.
    for (const [x0, y0, x1, y1] of boxes) {
      const set = keys(ellipsePixels({ x: x0, y: y0 }, { x: x1, y: y1 }))
      for (const key of set) {
        const [x, y] = key.split(',').map(Number)
        expect(set.has(`${x0 + x1 - x},${y}`)).toBe(true)
        expect(set.has(`${x},${y0 + y1 - y}`)).toBe(true)
      }
    }
  })

  it('forms a closed outline with no gaps', () => {
    // Every outline pixel needs at least two 8-neighbours, otherwise the ring has a hole.
    const set = keys(ellipsePixels({ x: 2, y: 3 }, { x: 40, y: 30 }))
    for (const key of set) {
      const [x, y] = key.split(',').map(Number)
      let neighbours = 0
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) if ((dx || dy) && set.has(`${x + dx},${y + dy}`)) neighbours++
      }
      expect(neighbours).toBeGreaterThanOrEqual(2)
    }
  })

  it('does not fill the interior', () => {
    // A filled ellipse would burn through the 1024 budget far faster than the outline the user chose.
    const set = keys(ellipsePixels({ x: 0, y: 0 }, { x: 40, y: 40 }))
    expect(set.has('20,20')).toBe(false)
    expect(set.size).toBeLessThan(200)
  })
})
