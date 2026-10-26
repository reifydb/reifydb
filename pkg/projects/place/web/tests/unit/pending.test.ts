// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { MAX_PENDING } from '@/geometry'
import { Pending } from '@/pending'

describe('Pending.remove', () => {
  it('removes only the targeted pixel and notifies', () => {
    // The eraser must not touch neighbouring pending pixels, and the canvas must redraw.
    const pending = new Pending()
    pending.add({ x: 1, y: 1, color: 3 })
    pending.add({ x: 2, y: 1, color: 4 })
    let calls = 0
    pending.subscribe(() => calls++)

    expect(pending.remove(1, 1)).toBe(true)

    expect(pending.get(1, 1)).toBeUndefined()
    expect(pending.get(2, 1)?.color).toBe(4)
    expect(pending.size).toBe(1)
    expect(calls).toBe(1)
  })

  it('frees a slot once pending is full', () => {
    // The eraser is the way to make room without clearing everything.
    const pending = new Pending()
    for (let i = 0; i < MAX_PENDING; i++) pending.add({ x: i, y: 0, color: 1 })
    expect(pending.add({ x: 0, y: 1, color: 1 })).toBe(false)

    pending.remove(5, 0)

    expect(pending.add({ x: 0, y: 1, color: 1 })).toBe(true)
  })

  it('reports false and stays silent for a pixel that is not pending', () => {
    // Erasing over committed art must be a no-op, never a write or a spurious redraw.
    const pending = new Pending()
    let calls = 0
    pending.subscribe(() => calls++)

    expect(pending.remove(9, 9)).toBe(false)
    expect(calls).toBe(0)
  })
})

describe('Pending.addAll', () => {
  const row = (count: number, y = 0, color = 2) => Array.from({ length: count }, (_, x) => ({ x, y, color }))

  it('adds the whole shape with a single notify', () => {
    // One release must redraw once, not once per pixel.
    const pending = new Pending()
    let calls = 0
    pending.subscribe(() => calls++)

    expect(pending.addAll(row(50))).toBe(true)

    expect(pending.size).toBe(50)
    expect(calls).toBe(1)
  })

  it('rejects the whole shape when it does not fit and leaves pending untouched', () => {
    // A half drawn shape committed to the shared canvas cannot be taken back.
    const pending = new Pending()
    pending.addAll(row(MAX_PENDING - 10, 0))
    let calls = 0
    pending.subscribe(() => calls++)

    expect(pending.addAll(row(11, 1))).toBe(false)

    expect(pending.size).toBe(MAX_PENDING - 10)
    expect(pending.get(0, 1)).toBeUndefined()
    expect(calls).toBe(0)
  })

  it('accepts a shape that exactly fills the remaining room', () => {
    // An off by one in the limit check would refuse a shape that fits.
    const pending = new Pending()
    pending.addAll(row(MAX_PENDING - 10, 0))

    expect(pending.addAll(row(10, 1))).toBe(true)
    expect(pending.size).toBe(MAX_PENDING)
  })

  it('does not charge pixels that are already pending', () => {
    // Redrawing over your own pending pixels must not count them twice against the limit.
    const pending = new Pending()
    pending.addAll(row(MAX_PENDING, 0, 2))

    expect(pending.addAll(row(20, 0, 5))).toBe(true)
    expect(pending.size).toBe(MAX_PENDING)
    expect(pending.get(3, 0)?.color).toBe(5)
  })

  it('clips pixels outside the canvas instead of rejecting the shape', () => {
    // A rectangle dragged past the edge should keep its visible part.
    const pending = new Pending()

    expect(pending.addAll([{ x: -1, y: 0, color: 2 }, { x: 0, y: 0, color: 2 }, { x: 0, y: -3, color: 2 }])).toBe(true)

    expect(pending.size).toBe(1)
    expect(pending.get(0, 0)?.color).toBe(2)
  })

  it('rejects the whole shape on an invalid color', () => {
    // A bad palette index would be refused by the commit, so it must not enter pending.
    const pending = new Pending()

    expect(pending.addAll([{ x: 0, y: 0, color: 2 }, { x: 1, y: 0, color: 99 }])).toBe(false)
    expect(pending.size).toBe(0)
  })
})
