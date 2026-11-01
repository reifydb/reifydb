// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import type { Agents } from '@/agents'
import { AgentsPage, ago } from '@/components/agents-page'

const NOW = 10_000_000_000

const agents: Agents = {
  painters: [
    { owner: 'b', name: 'agent-b', painted: 3120, last: NOW - 120_000 },
    { owner: 'a', name: 'agent-a', painted: 418, last: NOW - 3_600_000 },
  ],
  guests: { painters: 14, painted: 2034 },
}

describe('AgentsPage', () => {
  it('lists painters in the given order with pixels and relative time, and the guest summary', async () => {
    // The page must show exactly what summarize ranked; reordering or dropping guests misreports who painted.
    render(<AgentsPage load={async () => agents} now={() => NOW} />)

    const rows = await screen.findAllByRole('row')
    expect(rows.slice(1).map((row) => row.textContent)).toEqual(['1agent-b3,1202 min ago', '2agent-a4181 h ago'])
    expect(screen.getByText('Guests: 14 painters, 2,034 pixels')).toBeInTheDocument()
  })

  it('shows why loading failed', async () => {
    // A silent empty list would read as "nobody painted" when the query actually failed.
    render(<AgentsPage load={async () => Promise.reject(new Error('policy denied'))} />)
    expect(await screen.findByText('Could not load agents: policy denied')).toBeInTheDocument()
  })

  it('says nobody painted on an empty canvas', async () => {
    // An empty table with headers only looks broken.
    render(<AgentsPage load={async () => ({ painters: [], guests: { painters: 0, painted: 0 } })} />)
    expect(await screen.findByText('Nobody has painted yet.')).toBeInTheDocument()
  })

  it('reloads the list on refresh', async () => {
    // The list loads on demand, so refresh is the only way to see new painters without a reload.
    const load = vi
      .fn<() => Promise<Agents>>()
      .mockResolvedValueOnce({ painters: [], guests: { painters: 0, painted: 0 } })
      .mockResolvedValueOnce(agents)
    render(<AgentsPage load={load} now={() => NOW} />)
    await screen.findByText('Nobody has painted yet.')

    await userEvent.click(screen.getByRole('button', { name: 'Refresh' }))

    expect(await screen.findByText('agent-b')).toBeInTheDocument()
    expect(load).toHaveBeenCalledTimes(2)
  })
})

describe('ago', () => {
  it('turns a past time into a short relative label', () => {
    // Readers compare painters at a glance; exact timestamps would make that slow.
    expect(ago(NOW - 5_000, NOW)).toBe('just now')
    expect(ago(NOW - 120_000, NOW)).toBe('2 min ago')
    expect(ago(NOW - 3 * 3_600_000, NOW)).toBe('3 h ago')
    expect(ago(NOW - 2 * 86_400_000, NOW)).toBe('2 d ago')
    expect(ago(NOW + 60_000, NOW)).toBe('just now')
  })
})
