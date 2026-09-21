// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { ReifyError, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { probes } from '@/hooks/use-probes'
import { MonitorDetailPage } from '@/pages/monitors/detail.tsx'
import { monitors, results } from '@/store/queries'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'
import {
  caughtUp,
  createMonitor,
  identityOf,
  monitorInDb,
  monitorRegionsInDb,
  regionNamed,
  reportResult,
  resultRegionsInDb,
  routeParams,
  statusPagesOfMonitorInDb,
} from '../../support/monitors'
import { createStatusPage } from '../../support/status-pages'

vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/monitors')).monitorRouterMock())

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('monitor detail writes', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let owner: string
  let usEast: string
  let euWest: string

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
    owner = await identityOf(db, 'tester')
    usEast = await regionNamed(db, 'US East')
    euWest = await regionNamed(db, 'EU West')
    navigate.mockClear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  async function renderDetail(monitorId: string) {
    routeParams.monitorId = monitorId
    vi.mocked(client.command).mockClear()
    renderWithProviders(<MonitorDetailPage />, store)
    await caughtUp(client)
  }

  async function commandSettled(calls: number) {
    const command = vi.mocked(client.command)
    await waitFor(() => expect(command).toHaveBeenCalledTimes(calls))
    await act(async () => {
      await Promise.allSettled(command.mock.results.map((result) => result.value))
    })
  }

  it('pauses and resumes the monitor in the db without touching its regions or results', async () => {
    // Resending a kept region on pause would reset its status, and removing it would delete its results.
    const id = await createMonitor(client, 'alpha', [usEast])
    await reportResult(db, { monitorId: id, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 40 })
    await renderDetail(id)

    await userEvent.click(await screen.findByRole('button', { name: 'Pause' }))
    await commandSettled(1)
    await caughtUp(client)

    expect(await screen.findByRole('button', { name: 'Resume' })).toBeInTheDocument()
    expect(screen.getByText('Paused')).toBeInTheDocument()
    expect(await monitorInDb(db, id)).toMatchObject([{ name: 'alpha', enabled: false, status: 'up' }])
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'up' })
    expect(await resultRegionsInDb(db, id)).toEqual([usEast])

    await userEvent.click(screen.getByRole('button', { name: 'Resume' }))
    await commandSettled(2)
    await caughtUp(client)

    expect(await screen.findByRole('button', { name: 'Pause' })).toBeInTheDocument()
    expect(await monitorInDb(db, id)).toMatchObject([{ enabled: true, status: 'up' }])
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'up' })
  })

  it('deletes the monitor with its regions, results and status page entries, and nothing of another monitor', async () => {
    // The cascade must be scoped to the deleted monitor; an unscoped delete would take beta's rows with it.
    const alpha = await createMonitor(client, 'alpha', [usEast, euWest])
    const beta = await createMonitor(client, 'beta', [usEast])
    await reportResult(db, { monitorId: alpha, owner, regionId: euWest, success: true, statusCode: 200, responseMs: 40 })
    await reportResult(db, { monitorId: beta, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 40 })
    const page = await createStatusPage(client, 'public', 'Public', [alpha, beta])
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    await renderDetail(alpha)

    await userEvent.click(await screen.findByRole('button', { name: 'Delete' }))
    await commandSettled(1)

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/monitors' }))
    expect(await monitorInDb(db, alpha)).toEqual([])
    expect(await monitorRegionsInDb(db, alpha)).toEqual({})
    expect(await resultRegionsInDb(db, alpha)).toEqual([])
    expect(await statusPagesOfMonitorInDb(db, alpha)).toEqual([])
    expect(await monitorInDb(db, beta)).toMatchObject([{ name: 'beta' }])
    expect(await monitorRegionsInDb(db, beta)).toEqual({ [usEast]: 'up' })
    expect(await resultRegionsInDb(db, beta)).toEqual([usEast])
    expect(await statusPagesOfMonitorInDb(db, beta)).toEqual([page])
  })

  it('shows why a refused pause failed next to the buttons and writes nothing', async () => {
    // A refused pause must say why; otherwise the button silently does nothing and the user believes it paused.
    const id = await createMonitor(client, 'alpha', [usEast])
    await renderDetail(id)
    await db.commandAs(owner, 'CALL uptime::delete_monitor($id)', { id }, [])

    await userEvent.click(await screen.findByRole('button', { name: 'Pause' }))
    await commandSettled(1)

    expect(await screen.findByText(/^Failed to update monitor: .*monitor not found/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Pause' })).toBeInTheDocument()
    expect(await monitorInDb(db, id)).toEqual([])
  })

  it('shows the diagnostic of a refused delete without its code or label, and stays on the page', async () => {
    // Over the socket a refused CALL is a ReifyError; the user must read its message, never the code and RQL label around it.
    const id = await createMonitor(client, 'alpha', [usEast])
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    await renderDetail(id)
    vi.mocked(client.command).mockRejectedValueOnce(
      new ReifyError({
        id: 'req-1',
        type: 'Err',
        payload: {
          diagnostic: {
            code: 'ASSERT',
            message: 'monitor not found',
            label: 'this expression is false: is::some($monitor.id)',
            notes: [],
          },
        },
      }),
    )

    await userEvent.click(await screen.findByRole('button', { name: 'Delete' }))
    await commandSettled(1)

    expect(await screen.findByText('Failed to delete monitor: monitor not found')).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect(await monitorInDb(db, id)).toMatchObject([{ name: 'alpha' }])
  })

  it('says the monitor failed to load instead of spinning forever', async () => {
    // A failed monitors subscription never turns ready, so a page that only waits for ready would spin with nothing saying why.
    const id = await createMonitor(client, 'alpha', [usEast])
    store.fail(monitors, null, new Error('monitors subscription refused'))
    await renderDetail(id)

    expect(await screen.findByText('Failed to load monitor: monitors subscription refused')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Pause' })).not.toBeInTheDocument()
    expect(screen.queryByText('Monitor not found')).not.toBeInTheDocument()
  })

  it('says the checks failed to load instead of claiming none were recorded', async () => {
    // A failed results subscription read as empty would tell the user a monitor that is checking was never checked.
    const id = await createMonitor(client, 'alpha', [usEast])
    await reportResult(db, { monitorId: id, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 40 })
    store.fail(results, { monitor_id: id }, new Error('results subscription refused'))
    await renderDetail(id)

    expect(await screen.findByText('Failed to load checks: results subscription refused')).toBeInTheDocument()
    expect(screen.queryByText('No checks recorded yet')).not.toBeInTheDocument()
  })

  it('says the probe names failed to load while it still lists the checks', async () => {
    // A failed probes subscription must say so; otherwise every check silently shows no probe as if none ran it.
    const id = await createMonitor(client, 'alpha', [usEast])
    await reportResult(db, { monitorId: id, owner, regionId: usEast, success: true, statusCode: 207, responseMs: 40 })
    store.fail(probes, null, new Error('probes subscription refused'))
    await renderDetail(id)

    expect(await screen.findByText('Failed to load probes: probes subscription refused')).toBeInTheDocument()
    expect(await screen.findByText('HTTP 207')).toBeInTheDocument()
    expect(screen.queryByText('No checks recorded yet')).not.toBeInTheDocument()
  })
})
