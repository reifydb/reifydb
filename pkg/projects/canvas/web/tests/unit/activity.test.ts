// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { ACTIVITY_LIMIT, Activity } from '@/activity'
import { TILE_BYTES } from '@/pixels'

const RED = 2
const BLUE = 13

function tile(color = 0): Uint8Array {
  return new Uint8Array(TILE_BYTES).fill(color)
}

function paint(base: Uint8Array, count: number, color: number, from = 0): Uint8Array {
  const out = base.slice()
  for (let i = from; i < from + count; i++) out[i] = color
  return out
}

describe('Activity.put', () => {
  it('ignores the first load of a tile', () => {
    // Hydration delivers every tile once; reporting those would flood the feed with 16,384 fake edits.
    const activity = new Activity(() => 0)
    activity.put(3, 5, undefined, paint(tile(), 10, RED))

    expect(activity.latest()).toEqual([])
  })

  it('counts only the pixels that changed and names the main new colour', () => {
    // Counting all non-white pixels would report the whole drawing again on every edit.
    const activity = new Activity(() => 42)
    const before = paint(tile(), 100, BLUE)
    const after = paint(paint(before, 7, RED, 200), 3, BLUE, 300)
    activity.put(3, 5, before, after)

    expect(activity.latest()).toEqual([{ id: 0, tx: 3, ty: 5, px: 10, color: RED, at: 42 }])
  })

  it('skips an update that changes nothing', () => {
    // A re-sent identical tile, for example after reconnect, is not activity.
    const activity = new Activity(() => 0)
    activity.put(0, 0, tile(RED), tile(RED))

    expect(activity.latest()).toEqual([])
  })

  it('keeps the newest edits first and drops the oldest past the limit', () => {
    // Unbounded history would grow forever on a busy canvas.
    const activity = new Activity(() => 0)
    for (let i = 0; i <= ACTIVITY_LIMIT; i++) activity.put(i % 128, 0, tile(), paint(tile(), 1, RED))

    expect(activity.latest()).toHaveLength(ACTIVITY_LIMIT)
    expect(activity.latest()[0].id).toBe(ACTIVITY_LIMIT)
  })

  it('notifies once per real edit and not for ignored updates', () => {
    // A notify on hydration would re-render the feed thousands of times at load.
    const activity = new Activity(() => 0)
    let calls = 0
    activity.subscribe(() => calls++)

    activity.put(0, 0, undefined, tile(RED))
    activity.put(0, 0, tile(RED), tile(RED))
    activity.put(0, 0, tile(), tile(RED))

    expect(calls).toBe(1)
  })
})

describe('Activity windows', () => {
  it('sums pixels and edits only inside the window', () => {
    // A window that ignores time would show lifetime totals as "last minute".
    let now = 0
    const activity = new Activity(() => now)
    activity.put(0, 0, tile(), paint(tile(), 5, RED))
    now = 61_000
    activity.put(1, 0, tile(), paint(tile(), 7, RED))

    expect(activity.pixelsSince(60_000)).toBe(7)
    expect(activity.editsSince(60_000)).toBe(1)
  })
})
