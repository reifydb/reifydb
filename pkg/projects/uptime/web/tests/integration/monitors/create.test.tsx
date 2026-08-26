// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { MonitorNewPage } from '@/pages/monitors/new.tsx'
import { useRealtimeStore } from '@/store/realtime'
import { type Backend, type BackendCtor, loadBackend } from '../../support/backend'
import { renderWithProviders } from '../../support/render'
import { insertMonitorRql, uuid7 } from '../../support/rql'
import { navigate } from '../../support/router-mock'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let Engine: BackendCtor

beforeAll(() => {
  Engine = loadBackend()
})

// Stubs fetch, never the mutation hook, so apiFetch's real code path still executes.
function installFetchBridge(engine: Backend) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString()
      if (init?.method === 'POST' && url.endsWith('/monitors')) {
        const body = JSON.parse(init.body as string)
        const id = uuid7()
        engine.command(insertMonitorRql(id, body))
        return new Response(JSON.stringify({ id, ...body }), {
          status: 201,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      throw new Error(`unexpected fetch in test: ${init?.method ?? 'GET'} ${url}`)
    }),
  )
}

function renderPage() {
  return renderWithProviders(<MonitorNewPage />)
}

describe('create monitor flow', () => {
  let engine: Backend

  beforeEach(() => {
    engine = new Engine(1)
    useRealtimeStore.setState({ regions: { 'region-1': { id: 'region-1', label: 'US East' } } })
    installFetchBridge(engine)
    navigate.mockClear()
  })

  afterEach(() => {
    // must run before RTL's own unmount, otherwise this store write hits a still-mounted subscriber outside act()
    act(() => {
      useRealtimeStore.getState().reset()
    })
    vi.unstubAllGlobals()
  })

  it('creates a monitor through the real form and it lands in the real uptime schema', async () => {
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'reifydb.com')
    await userEvent.type(screen.getByLabelText('URL'), 'https://reifydb.com/health')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [{ params }] = navigate.mock.calls[0]
    expect(params.monitorId).toBeTruthy()

    // Asserts against the real engine, not the mock response - otherwise a broken migration would go undetected.
    const rows = JSON.parse(
      engine.query(
        'from uptime::monitors filter { name == "reifydb.com" } map { name, kind, target, status }',
      ),
    )
    expect(rows).toEqual([
      { name: 'reifydb.com', kind: 'http', target: 'https://reifydb.com/health', status: 'unknown' },
    ])
  })

  it('blocks submission client-side when no region is selected, never touching the network', async () => {
    useRealtimeStore.setState({ regions: {} })
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'reifydb.com')
    await userEvent.type(screen.getByLabelText('URL'), 'https://reifydb.com')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    expect(await screen.findByText(/select at least one region/i)).toBeInTheDocument()
    expect(globalThis.fetch).not.toHaveBeenCalled()
    expect(navigate).not.toHaveBeenCalled()
  })
})
