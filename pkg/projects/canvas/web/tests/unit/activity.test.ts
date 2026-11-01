// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { ACTIVITY_LIMIT, Activity, STREAK_LIMIT, STREAK_MS } from '@/activity'

const RED = 2
const BLUE = 13

function edit(tx: number, ty: number, colors: number[]): Uint8Array {
  // Must match the commit tile blob layout, or these tests check a format the server never sends.
  const out = new Uint8Array(2 + colors.length * 3)
  out[0] = tx
  out[1] = ty
  colors.forEach((color, i) => {
    out[2 + i * 3] = i & 0xff
    out[3 + i * 3] = i >> 8
    out[4 + i * 3] = color
  })
  return out
}

describe('Activity.put', () => {
  it('logs the painter name, tile, pixel count and main colour of an edit row', () => {
    // The name is the point of the log; a missing or swapped field credits the wrong painter or tile.
    const activity = new Activity(() => 42)
    activity.put('alice', edit(3, 5, [RED, RED, BLUE, RED]), 4)

    expect(activity.latest()).toEqual([{ id: 0, name: 'alice', tx: 3, ty: 5, px: 4, color: RED, at: 42 }])
  })

  it('reads the colour byte, not the index bytes, of each pixel', () => {
    // Index bytes at 300 hold 44 and 1; reading them as colours would name a colour nobody painted.
    const activity = new Activity(() => 0)
    const blob = new Uint8Array([0, 0, 0x2c, 0x01, BLUE, 0x2c, 0x01, BLUE])
    activity.put('bob', blob, 2)

    expect(activity.latest()[0].color).toBe(BLUE)
  })

  it('skips an edit row that painted nothing', () => {
    // An empty edit is not activity and would show "painted 0 px".
    const activity = new Activity(() => 0)
    activity.put('alice', edit(0, 0, []), 0)

    expect(activity.latest()).toEqual([])
  })

  it('fails loud on a malformed edit blob', () => {
    // A blob that is not 2 + 3n bytes means the wire format drifted; guessing would log garbage tiles.
    const activity = new Activity(() => 0)
    expect(() => activity.put('alice', new Uint8Array([1, 2, 3, 4]), 1)).toThrow()
  })

  it('keeps the newest edits first and drops the oldest past the limit', () => {
    // Unbounded history would grow forever on a busy canvas.
    const activity = new Activity(() => 0)
    for (let i = 0; i <= ACTIVITY_LIMIT; i++) activity.put('alice', edit(i % 128, 0, [RED]), 1)

    expect(activity.latest()).toHaveLength(ACTIVITY_LIMIT)
    expect(activity.latest()[0].id).toBe(ACTIVITY_LIMIT)
  })

  it('notifies once per logged edit and not for skipped ones', () => {
    // A notify for an empty edit would re-render the feed for nothing.
    const activity = new Activity(() => 0)
    let calls = 0
    activity.subscribe(() => calls++)

    activity.put('alice', edit(0, 0, []), 0)
    activity.put('alice', edit(0, 0, [RED]), 1)

    expect(calls).toBe(1)
  })
})

describe('Activity.streaks', () => {
  const view = (activity: Activity) =>
    activity.streaks().map(({ name, px, tiles, tx, ty, color, at }) => ({ name, px, tiles: tiles.length, tx, ty, color, at }))

  it('merges a painter paint-by-pixel loop into one line', () => {
    // A 1 px loop must not flood the log; one painter within the window is one line that grows.
    let now = 0
    const activity = new Activity(() => now)
    for (let i = 0; i < 5; i++) {
      now = i * 1000
      activity.put('alice', edit(0, 0, [RED]), 1)
    }

    expect(view(activity)).toEqual([{ name: 'alice', px: 5, tiles: 1, tx: 0, ty: 0, color: RED, at: 4000 }])
    expect(activity.latest()).toHaveLength(5)
  })

  it('counts distinct tiles and reports the newest tile and main colour of the streak', () => {
    // Pixel totals hide spread; a repeated tile counted twice or a stale colour would misdescribe the streak.
    const activity = new Activity(() => 0)
    activity.put('alice', edit(1, 2, [BLUE]), 1)
    activity.put('alice', edit(3, 4, [RED, RED]), 2)
    activity.put('alice', edit(1, 2, [RED]), 1)

    expect(view(activity)).toEqual([{ name: 'alice', px: 4, tiles: 2, tx: 1, ty: 2, color: RED, at: 0 }])
  })

  it('starts a new line for another painter or after a pause longer than the window', () => {
    // Merging across painters credits the wrong name; merging across a long pause hides that a new session began.
    let now = 0
    const activity = new Activity(() => now)
    activity.put('alice', edit(0, 0, [RED]), 1)
    now = 1000
    activity.put('bob', edit(0, 0, [RED]), 1)
    now = 2000
    activity.put('bob', edit(0, 0, [RED]), 1)
    now = 2000 + STREAK_MS + 1
    activity.put('bob', edit(0, 0, [RED]), 1)

    expect(view(activity).map(({ name, px }) => [name, px])).toEqual([
      ['bob', 1],
      ['bob', 2],
      ['alice', 1],
    ])
  })

  it('merges exactly at the window edge', () => {
    // The window is inclusive; an off-by-one would split a steady painter into two lines.
    let now = 0
    const activity = new Activity(() => now)
    activity.put('alice', edit(0, 0, [RED]), 1)
    now = STREAK_MS
    activity.put('alice', edit(0, 0, [RED]), 1)

    expect(activity.streaks()).toHaveLength(1)
  })

  it('keeps a streak id stable while it grows and hands out a fresh snapshot each time', () => {
    // A changing id would replay the row animation on every pixel; a reused array would not re-render the feed.
    const activity = new Activity(() => 0)
    activity.put('alice', edit(0, 0, [RED]), 1)
    const first = activity.streaks()
    activity.put('alice', edit(0, 0, [RED]), 1)
    const second = activity.streaks()

    expect(second).not.toBe(first)
    expect(second[0].id).toBe(first[0].id)
    expect(first[0].px).toBe(1)
  })

  it('keeps the newest streak lines up to their own limit, separate from the raw edits', () => {
    // The log scrolls through STREAK_LIMIT lines while pings only need ACTIVITY_LIMIT raw rows; one cap for both loses history.
    let now = 0
    const activity = new Activity(() => now)
    for (let i = 0; i <= STREAK_LIMIT; i++) {
      now = i * (STREAK_MS + 1)
      activity.put(`painter-${i}`, edit(0, 0, [RED]), 1)
    }

    expect(activity.streaks()).toHaveLength(STREAK_LIMIT)
    expect(activity.streaks()[0].name).toBe(`painter-${STREAK_LIMIT}`)
    expect(activity.latest()).toHaveLength(ACTIVITY_LIMIT)
  })
})
