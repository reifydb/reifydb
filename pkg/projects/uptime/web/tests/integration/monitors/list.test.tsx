// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { DashboardPage } from '@/pages/dashboard'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { addRegion, caughtUp, createMonitor, regionNamed } from '../../support/monitors'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('monitors list', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
  })

  async function renderPage() {
    renderWithProviders(<DashboardPage />, store)
    await caughtUp(client)
  }

  function setRegionStatus(monitorId: string, regionId: string, status: string) {
    return db.commandRoot(
      'update uptime::monitor_regions { status: $status } filter { monitor_id == $monitor_id and region_id == $region_id }',
      { monitor_id: monitorId, region_id: regionId, status },
      [],
    )
  }

  it('shows a loading indicator before monitors are ready', () => {
    renderWithProviders(<DashboardPage />, store)
    expect(screen.getByText('Loading')).toBeInTheDocument()
  })

  it('shows an empty state with a create-monitor CTA when there are no monitors', async () => {
    await renderPage()

    expect(await screen.findByRole('heading', { name: 'No monitors yet' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /create monitor/i })).toHaveAttribute(
      'href',
      '/monitors/new',
    )
  })

  it('renders a monitor row with its name, type, target and last-checked time', async () => {
    await createMonitor(client, 'alpha-api')
    await renderPage()

    const row = await screen.findByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('HTTP')).toBeInTheDocument()
    expect(within(row).getByText('https://alpha-api.example.com/health')).toBeInTheDocument()
    expect(within(row).getByText('never')).toBeInTheDocument()
  })

  it('links the monitor name to its detail page', async () => {
    const id = await createMonitor(client, 'alpha-api')
    await renderPage()

    expect(await screen.findByRole('link', { name: 'alpha-api' })).toHaveAttribute(
      'href',
      `/monitors/${id}`,
    )
  })

  it('shows Paused instead of a status badge for a disabled monitor', async () => {
    const id = await createMonitor(client, 'beta-db')
    await db.commandRoot('update uptime::monitors { enabled: false, status: "down" } filter { id == $id }', { id }, [])
    await renderPage()

    const row = await screen.findByRole('row', { name: /beta-db/i })
    expect(within(row).getByText('Paused')).toBeInTheDocument()
    expect(within(row).queryByText('Down')).not.toBeInTheDocument()
  })

  it('shows the up-region fraction based on actual region statuses', async () => {
    const usEast = await regionNamed(db, 'US East')
    const euWest = await regionNamed(db, 'EU West')
    const apSouth = await addRegion(db, 'AP South')
    const id = await createMonitor(client, 'alpha-api', [usEast, euWest, apSouth])
    await db.commandRoot('update uptime::monitors { status: "degraded" } filter { id == $id }', { id }, [])
    await setRegionStatus(id, usEast, 'up')
    await setRegionStatus(id, euWest, 'up')
    await setRegionStatus(id, apSouth, 'down')
    await renderPage()

    const row = await screen.findByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('2/3')).toBeInTheDocument()
  })

  it('only offers the region-expand toggle for monitors with regions, and it reveals region rows', async () => {
    await createMonitor(client, 'alpha-api', [await regionNamed(db, 'US East')])
    await createMonitor(client, 'beta-db')
    await renderPage()

    const rowWithoutRegions = await screen.findByRole('row', { name: /beta-db/i })
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
