// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { DashboardPage } from '@/pages/dashboard'
import { useRealtimeStore } from '@/store/realtime'
import type { Monitor } from '@/lib/types'
import { baseMonitor } from '../../support/fixtures'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

function renderPage() {
  return render(<DashboardPage />)
}

describe('monitors list', () => {
  afterEach(() => {
    // must run before RTL's own unmount, otherwise this store write hits a still-mounted subscriber outside act()
    act(() => {
      useRealtimeStore.getState().reset()
    })
  })

  it('shows a loading indicator before monitors are ready', () => {
    renderPage()
    expect(screen.getByText('Loading')).toBeInTheDocument()
  })

  it('shows an empty state with a create-monitor CTA when there are no monitors', () => {
    useRealtimeStore.setState({ monitorsReady: true, monitors: {} })
    renderPage()

    expect(screen.getByRole('heading', { name: 'No monitors yet' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /create monitor/i })).toHaveAttribute(
      'href',
      '/monitors/new',
    )
  })

  it('renders a monitor row with its name, type, target and last-checked time', () => {
    const monitor: Monitor = { ...baseMonitor, id: 'mon-1', name: 'alpha-api' }
    useRealtimeStore.setState({ monitorsReady: true, monitors: { [monitor.id]: monitor } })
    renderPage()

    const row = screen.getByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('HTTP')).toBeInTheDocument()
    expect(within(row).getByText(monitor.target)).toBeInTheDocument()
    expect(within(row).getByText('never')).toBeInTheDocument()
  })

  it('links the monitor name to its detail page', () => {
    const monitor: Monitor = { ...baseMonitor, id: 'mon-42', name: 'alpha-api' }
    useRealtimeStore.setState({ monitorsReady: true, monitors: { [monitor.id]: monitor } })
    renderPage()

    expect(screen.getByRole('link', { name: 'alpha-api' })).toHaveAttribute(
      'href',
      '/monitors/mon-42',
    )
  })

  it('shows Paused instead of a status badge for a disabled monitor', () => {
    const monitor: Monitor = {
      ...baseMonitor,
      id: 'mon-1',
      name: 'beta-db',
      enabled: false,
      status: 'down',
    }
    useRealtimeStore.setState({ monitorsReady: true, monitors: { [monitor.id]: monitor } })
    renderPage()

    const row = screen.getByRole('row', { name: /beta-db/i })
    expect(within(row).getByText('Paused')).toBeInTheDocument()
    expect(within(row).queryByText('Down')).not.toBeInTheDocument()
  })

  it('shows the up-region fraction based on actual region statuses', () => {
    const monitor: Monitor = { ...baseMonitor, id: 'mon-1', name: 'alpha-api', status: 'degraded' }
    useRealtimeStore.setState({
      monitorsReady: true,
      monitors: { [monitor.id]: monitor },
      regions: {
        'r-us': { id: 'r-us', label: 'US East' },
        'r-eu': { id: 'r-eu', label: 'EU West' },
        'r-ap': { id: 'r-ap', label: 'AP South' },
      },
      monitorRegions: {
        'mon-1|r-us': {
          monitor_id: 'mon-1',
          region_id: 'r-us',
          status: 'up',
          last_checked_at: null,
          consecutive_failures: 0,
        },
        'mon-1|r-eu': {
          monitor_id: 'mon-1',
          region_id: 'r-eu',
          status: 'up',
          last_checked_at: null,
          consecutive_failures: 0,
        },
        'mon-1|r-ap': {
          monitor_id: 'mon-1',
          region_id: 'r-ap',
          status: 'down',
          last_checked_at: null,
          consecutive_failures: 2,
        },
      },
    })
    renderPage()

    const row = screen.getByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('2/3')).toBeInTheDocument()
  })

  it('only offers the region-expand toggle for monitors with regions, and it reveals region rows', async () => {
    const withRegions: Monitor = { ...baseMonitor, id: 'mon-1', name: 'alpha-api' }
    const withoutRegions: Monitor = { ...baseMonitor, id: 'mon-2', name: 'beta-db' }
    useRealtimeStore.setState({
      monitorsReady: true,
      monitors: { [withRegions.id]: withRegions, [withoutRegions.id]: withoutRegions },
      regions: { 'r-us': { id: 'r-us', label: 'US East' } },
      monitorRegions: {
        'mon-1|r-us': {
          monitor_id: 'mon-1',
          region_id: 'r-us',
          status: 'up',
          last_checked_at: null,
          consecutive_failures: 0,
        },
      },
    })
    renderPage()

    const rowWithoutRegions = screen.getByRole('row', { name: /beta-db/i })
    expect(
      within(rowWithoutRegions).queryByRole('button', { name: /regions/i }),
    ).not.toBeInTheDocument()

    const toggle = screen.getByRole('button', { name: 'Expand regions' })
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByText('US East')).not.toBeInTheDocument()

    await userEvent.click(toggle)

    expect(screen.getByRole('button', { name: 'Collapse regions' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    expect(screen.getByText('US East')).toBeInTheDocument()
  })
})
