// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Option, type Store } from '@reifydb/react'
import { DashboardPage } from '@/pages/dashboard'
import { monitorRegions, monitors, regions, type MonitorRow } from '@/store/queries'
import { baseMonitor } from '../../support/fixtures'
import { renderWithProviders, seededStore } from '../../support/store'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let store: Store

beforeEach(() => {
  store = seededStore()
})

function renderPage() {
  return renderWithProviders(<DashboardPage />, store)
}

describe('monitors list', () => {
  it('shows a loading indicator before monitors are ready', () => {
    renderPage()
    expect(screen.getByText('Loading')).toBeInTheDocument()
  })

  it('shows an empty state with a create-monitor CTA when there are no monitors', () => {
    store.seed(monitors.rql, null, monitors.shape, [])
    renderPage()

    expect(screen.getByRole('heading', { name: 'No monitors yet' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /create monitor/i })).toHaveAttribute(
      'href',
      '/monitors/new',
    )
  })

  it('renders a monitor row with its name, type, target and last-checked time', () => {
    const monitor: MonitorRow = { ...baseMonitor, id: 'mon-1', name: 'alpha-api' }
    store.seed(monitors.rql, null, monitors.shape, [monitor])
    renderPage()

    const row = screen.getByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('HTTP')).toBeInTheDocument()
    expect(within(row).getByText(monitor.target)).toBeInTheDocument()
    expect(within(row).getByText('never')).toBeInTheDocument()
  })

  it('links the monitor name to its detail page', () => {
    const monitor: MonitorRow = { ...baseMonitor, id: 'mon-42', name: 'alpha-api' }
    store.seed(monitors.rql, null, monitors.shape, [monitor])
    renderPage()

    expect(screen.getByRole('link', { name: 'alpha-api' })).toHaveAttribute(
      'href',
      '/monitors/mon-42',
    )
  })

  it('shows Paused instead of a status badge for a disabled monitor', () => {
    const monitor: MonitorRow = {
      ...baseMonitor,
      id: 'mon-1',
      name: 'beta-db',
      enabled: false,
      status: 'down',
    }
    store.seed(monitors.rql, null, monitors.shape, [monitor])
    renderPage()

    const row = screen.getByRole('row', { name: /beta-db/i })
    expect(within(row).getByText('Paused')).toBeInTheDocument()
    expect(within(row).queryByText('Down')).not.toBeInTheDocument()
  })

  it('shows the up-region fraction based on actual region statuses', () => {
    const monitor: MonitorRow = { ...baseMonitor, id: 'mon-1', name: 'alpha-api', status: 'degraded' }
    store.seed(monitors.rql, null, monitors.shape, [monitor])
    store.seed(regions.rql, null, regions.shape, [
      { id: 'r-us', label: 'US East' },
      { id: 'r-eu', label: 'EU West' },
      { id: 'r-ap', label: 'AP South' },
    ])
    store.seed(monitorRegions.rql, null, monitorRegions.shape, [
      {
        monitorId: 'mon-1',
        regionId: 'r-us',
        status: 'up',
        lastCheckedAt: Option.none('DateTime'),
        consecutiveFailures: 0,
      },
      {
        monitorId: 'mon-1',
        regionId: 'r-eu',
        status: 'up',
        lastCheckedAt: Option.none('DateTime'),
        consecutiveFailures: 0,
      },
      {
        monitorId: 'mon-1',
        regionId: 'r-ap',
        status: 'down',
        lastCheckedAt: Option.none('DateTime'),
        consecutiveFailures: 2,
      },
    ])
    renderPage()

    const row = screen.getByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('2/3')).toBeInTheDocument()
  })

  it('only offers the region-expand toggle for monitors with regions, and it reveals region rows', async () => {
    const withRegions: MonitorRow = { ...baseMonitor, id: 'mon-1', name: 'alpha-api' }
    const withoutRegions: MonitorRow = { ...baseMonitor, id: 'mon-2', name: 'beta-db' }
    store.seed(monitors.rql, null, monitors.shape, [withRegions, withoutRegions])
    store.seed(regions.rql, null, regions.shape, [{ id: 'r-us', label: 'US East' }])
    store.seed(monitorRegions.rql, null, monitorRegions.shape, [
      {
        monitorId: 'mon-1',
        regionId: 'r-us',
        status: 'up',
        lastCheckedAt: Option.none('DateTime'),
        consecutiveFailures: 0,
      },
    ])
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
