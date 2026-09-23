// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { DashboardPage } from '@/pages/dashboard'
import { MonitorDetailPage } from '@/pages/monitors/detail.tsx'
import { results } from '@/store/queries'
import { loadBackend } from '../../support/backend'
import { bridgeStore, refusingStore, renderWithProviders } from '../../support/store'
import {
  caughtUp,
  createMonitor,
  identityOf,
  insertResults,
  regionNamed,
  reportResult,
  routeParams,
} from '../../support/monitors'

vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/monitors')).monitorRouterMock())

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('per-monitor results over the batched store', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let identity: string
  let owner: string
  let usEast: string
  let alpha: string
  let beta: string

  beforeEach(async () => {
    db = create()
    ;({ store, client, identity } = await bridgeStore(db, 'tester'))
    owner = await identityOf(db, 'tester')
    usEast = await regionNamed(db, 'US East')
    alpha = await createMonitor(db, identity, 'alpha', [usEast])
    beta = await createMonitor(db, identity, 'beta', [usEast])
  })

  it('shows a result reported for this monitor live, and never one reported for another monitor', async () => {
    // A monitor page that took results from every monitor would show beta's 503 among alpha's checks.
    routeParams.monitorId = alpha
    renderWithProviders(<MonitorDetailPage />, store)
    await caughtUp(client)
    expect(await screen.findByText('No checks recorded yet')).toBeInTheDocument()

    await reportResult(db, { monitorId: beta, owner, regionId: usEast, success: false, statusCode: 503, responseMs: 90 })
    await reportResult(db, { monitorId: alpha, owner, regionId: usEast, success: true, statusCode: 201, responseMs: 40 })
    await caughtUp(client)

    await waitFor(() => expect(screen.getByText('HTTP 201')).toBeInTheDocument())
    expect(screen.queryByText('HTTP 503')).not.toBeInTheDocument()
    expect(screen.queryByText('No checks recorded yet')).not.toBeInTheDocument()
  })

  it('hydrates only this monitor results that existed before the page mounted', async () => {
    // Hydration runs the parameterised query inside the page batch; a dropped $monitor_id leaves it empty or unscoped.
    await reportResult(db, { monitorId: alpha, owner, regionId: usEast, success: true, statusCode: 201, responseMs: 40 })
    await reportResult(db, { monitorId: beta, owner, regionId: usEast, success: false, statusCode: 503, responseMs: 90 })
    routeParams.monitorId = alpha

    renderWithProviders(<MonitorDetailPage />, store)
    await caughtUp(client)

    expect(await screen.findByText('HTTP 201')).toBeInTheDocument()
    expect(screen.queryByText('HTTP 503')).not.toBeInTheDocument()
  })

  it('still shows the newest check once a monitor has more results than the page keeps', async () => {
    // A monitor checked every minute passes 200 results within hours; the page must never go blank after that.
    await insertResults(db, {
      monitorId: alpha,
      owner,
      regionId: usEast,
      count: 200,
      statusCode: 200,
      checkedAt: new Date('2026-01-01T00:00:00Z'),
    })
    await insertResults(db, { monitorId: alpha, owner, regionId: usEast, count: 1, statusCode: 299, checkedAt: new Date() })
    routeParams.monitorId = alpha

    renderWithProviders(<MonitorDetailPage />, store)
    await caughtUp(client)

    expect(await screen.findByText('HTTP 299')).toBeInTheDocument()
  })

  it('averages an expanded dashboard row over its own monitor results only', async () => {
    // With results shared across monitors, alpha's US East row would average 100 ms and 300 ms into 200 ms.
    await reportResult(db, { monitorId: alpha, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 100 })
    await reportResult(db, { monitorId: beta, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 300 })

    renderWithProviders(<DashboardPage />, store)
    await caughtUp(client)
    const alphaRow = await screen.findByRole('row', { name: /alpha/i })
    await userEvent.click(within(alphaRow).getByRole('button', { name: 'Expand regions' }))
    await caughtUp(client)

    const regionRow = await screen.findByRole('row', { name: /US East/ })
    await waitFor(() => expect(within(regionRow).getByText('100 ms')).toBeInTheDocument())
    expect(within(regionRow).queryByText('200 ms')).not.toBeInTheDocument()
  })

  it('says the checks failed to load in an expanded dashboard row, and only for that monitor', async () => {
    // A failed results subscription read as empty would show a checking monitor with no latency and no history.
    await reportResult(db, { monitorId: alpha, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 100 })
    await reportResult(db, { monitorId: beta, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 300 })
    store = refusingStore(client, results, { monitor_id: alpha }, new Error('results subscription refused'))

    renderWithProviders(<DashboardPage />, store)
    await caughtUp(client)
    await userEvent.click(within(await screen.findByRole('row', { name: /alpha/i })).getByRole('button', { name: 'Expand regions' }))
    await userEvent.click(within(screen.getByRole('row', { name: /beta/i })).getByRole('button', { name: 'Expand regions' }))
    await caughtUp(client)

    expect(await screen.findByText('Failed to load checks: results subscription refused')).toBeInTheDocument()
    expect(screen.getAllByText(/^Failed to load checks/)).toHaveLength(1)
    const betaRegion = await screen.findByRole('row', { name: /US East/ })
    await waitFor(() => expect(within(betaRegion).getByText('300 ms')).toBeInTheDocument())
  })
})
