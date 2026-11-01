// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { summarize } from '@/agents'

const row = (owner: string, name: string, painted: number, last: number) => ({ owner, name, painted, last })

describe('summarize', () => {
  it('lists named painters with most pixels first, then the most recent on a tie', () => {
    // The list is a leaderboard; any other order buries the biggest painters.
    const agents = summarize([row('a', 'alpha', 10, 1), row('b', 'beta', 30, 1), row('c', 'gamma', 10, 5)])
    expect(agents.painters.map((p) => p.name)).toEqual(['beta', 'gamma', 'alpha'])
  })

  it('merges the rows of one owner and shows its newest name', () => {
    // A guest who signed up has rows under both names; showing the old one hides who they are now.
    const agents = summarize([row('o', 'guest:01a1', 5, 100), row('o', 'alice', 7, 200)])
    expect(agents.painters).toEqual([{ owner: 'o', name: 'alice', painted: 12, last: 200 }])
    expect(agents.guests).toEqual({ painters: 0, painted: 0 })
  })

  it('keeps the newest name whatever order the rows arrive in', () => {
    // Aggregate output has no order guarantee, so the newest name must come from last, not position.
    const agents = summarize([row('o', 'alice', 7, 200), row('o', 'guest:01a1', 5, 100)])
    expect(agents.painters[0].name).toBe('alice')
    expect(agents.painters[0].painted).toBe(12)
  })

  it('folds guests into one summary', () => {
    // Guest ids mean nothing to a reader, but their activity still counts.
    const agents = summarize([row('g1', 'guest:1', 4, 1), row('g2', 'guest:2', 6, 2), row('a', 'alpha', 1, 3)])
    expect(agents.painters.map((p) => p.name)).toEqual(['alpha'])
    expect(agents.guests).toEqual({ painters: 2, painted: 10 })
  })

  it('treats an owner whose newest name is a guest name as a guest', () => {
    // Only the newest name decides; a stale real name must not keep a guest on the list.
    const agents = summarize([row('o', 'alice', 3, 1), row('o', 'guest:1', 2, 9)])
    expect(agents.painters).toEqual([])
    expect(agents.guests).toEqual({ painters: 1, painted: 5 })
  })

  it('reads numbers and ids wrapped in value objects', () => {
    // The client can hand back typed values as { value }; reading them raw would show [object Object].
    const agents = summarize([{ owner: { value: 'o' }, name: { value: 'alpha' }, painted: { value: 3n }, last: { value: 7 } }])
    expect(agents.painters).toEqual([{ owner: 'o', name: 'alpha', painted: 3, last: 7 }])
  })

  it('answers an empty list when nothing was painted', () => {
    // A fresh canvas must render an empty list, not fail.
    expect(summarize([])).toEqual({ painters: [], guests: { painters: 0, painted: 0 } })
  })
})
