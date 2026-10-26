// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { clampView } from '@/components/canvas-view'

const screen = { width: 1000, height: 800 }

describe('clampView', () => {
  it('lets the top-left edge go 64 px past the screen edge and no further', () => {
    // Without the margin the edge sits on the screen edge and the end of the canvas is invisible.
    expect(clampView({ zoom: 1, x: 500, y: 500 }, screen)).toEqual({ zoom: 1, x: 64, y: 64 })
  })

  it('lets the bottom-right edge go 64 px past the screen edge and no further', () => {
    // A sign slip here would let the canvas drift off screen forever.
    expect(clampView({ zoom: 1, x: -100000, y: -100000 }, screen)).toEqual({ zoom: 1, x: 1000 - 8192 - 64, y: 800 - 8192 - 64 })
  })

  it('keeps the same 64 screen px margin at max zoom', () => {
    // A margin in canvas px would leave the whole screen empty at zoom 32.
    expect(clampView({ zoom: 32, x: 500, y: -1e9 }, screen)).toEqual({ zoom: 32, x: 64, y: 800 - 8192 * 32 - 64 })
  })

  it('still centres a canvas smaller than the screen', () => {
    // The margin must not shift a canvas that already fits.
    expect(clampView({ zoom: 1, x: 0, y: 0 }, { width: 10000, height: 9000 })).toEqual({ zoom: 1, x: 904, y: 404 })
  })
})
