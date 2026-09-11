// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { PublicStatusPage } from '@/pages/public-status'
import type { PublicStatus } from '@/lib/types'

vi.mock('@tanstack/react-router', () => ({ useParams: () => ({ slug: 'acme' }) }))

const page: PublicStatus = {
  title: 'Acme status',
  slug: 'acme',
  monitors: [
    {
      name: 'api.acme.com',
      status: 'up',
      uptime_24h: 1,
      last_checked_at: null,
      daily: [{ day: '2026-09-10', total: 10, up: 10 }],
      regions: [],
    },
  ],
}

const fetchMock = vi.fn()

function reply(status: number, body: unknown) {
  return { status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) }
}

async function settle(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('public status page', () => {
  it('renders the title and monitors of the slug in the route', async () => {
    // The page must request exactly the slug from the route and render what that route returns.
    fetchMock.mockResolvedValue(reply(200, page))

    render(<PublicStatusPage />)
    await settle(0)

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/public/status/acme',
      expect.objectContaining({ method: 'GET' }),
    )
    expect(screen.getByRole('heading', { name: 'Acme status' })).toBeInTheDocument()
    expect(screen.getByText('api.acme.com')).toBeInTheDocument()
    expect(screen.getByText('All systems operational')).toBeInTheDocument()
  })

  it('shows the not-found state after a single request for an unknown slug', async () => {
    // A 404 is final and must never be retried or polled; otherwise a dead link hits the server every minute forever.
    fetchMock.mockResolvedValue(reply(404, { error: 'status page not found' }))

    render(<PublicStatusPage />)
    await settle(0)

    expect(screen.getByText('This status page does not exist.')).toBeInTheDocument()
    await settle(150_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(screen.queryByText('Failed to load status page.')).not.toBeInTheDocument()
  })

  it('retries any other failure exactly three times before showing the error', async () => {
    // Exactly three retries: fewer turns one blip into an error page, more keeps hammering a failing server.
    fetchMock.mockResolvedValue(reply(503, { error: 'unavailable' }))

    render(<PublicStatusPage />)
    await settle(0)

    expect(screen.queryByText('Failed to load status page.')).not.toBeInTheDocument()
    await settle(30_000)
    expect(fetchMock).toHaveBeenCalledTimes(4)
    expect(screen.getByText('Failed to load status page.')).toBeInTheDocument()
  })

  it('refetches the page every minute', async () => {
    // Without the refresh a page left open on a wall would show a stale status forever.
    const down = { ...page, monitors: [{ ...page.monitors[0], status: 'down' as const }] }
    fetchMock.mockResolvedValueOnce(reply(200, page)).mockResolvedValue(reply(200, down))

    render(<PublicStatusPage />)
    await settle(0)
    expect(screen.getByText('All systems operational')).toBeInTheDocument()

    await settle(59_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    await settle(1_000)
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(screen.getByText('1 of 1 systems down')).toBeInTheDocument()
  })
})
