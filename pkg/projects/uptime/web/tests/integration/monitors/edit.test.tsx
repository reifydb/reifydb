// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from 'react'
import { act, renderHook, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Shape, Store, StoreProvider, rql, type StoreClient } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { useCreateMonitor, useUpdateMonitor } from '@/hooks/use-monitors'
import type { MonitorInput } from '@/lib/types'
import { MonitorEditPage } from '@/pages/monitors/edit.tsx'
import { STORE_OPTIONS } from '@/store/client'
import { monitorRegions, monitors, regions } from '@/store/queries'
import { loadBackend } from '../../support/backend'
import { commandRoot, queryRoot } from '../../support/db'
import { bridgeStore, refusingStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'
import {
  addRegion,
  caughtUp,
  createMonitor,
  identityOf,
  monitorInDb,
  monitorRegionsInDb,
  regionNamed,
  reportResult,
  resultRegionsInDb,
  routeParams,
} from '../../support/monitors'

vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/monitors')).monitorRouterMock())

const DELETE_REGION = rql.write([])<{ id: string }>`delete uptime::regions filter { id == $id }`

const monitorIds = rql(Shape.object({ id: Shape.uuid7() }))`from uptime::monitors map { id }`

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('edit monitor flow', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let identity: string
  let owner: string
  let usEast: string
  let euWest: string

  beforeEach(async () => {
    db = create()
    ;({ store, client, identity } = await bridgeStore(db, 'tester'))
    owner = await identityOf(db, 'tester')
    usEast = await regionNamed(db, 'US East')
    euWest = await regionNamed(db, 'EU West')
    navigate.mockClear()
  })

  async function renderEdit(monitorId: string) {
    routeParams.monitorId = monitorId
    renderWithProviders(<MonitorEditPage />, store)
    await caughtUp(client)
    const name = await screen.findByLabelText('Name')
    await caughtUp(client)
    return name
  }

  it('saves the edited fields and adds or removes only the regions that changed', async () => {
    // Re-adding the kept region would reset its status to unknown, and removing it would delete its results.
    const apSouth = await addRegion(db, 'AP South')
    const id = await createMonitor(db, identity, 'alpha', [usEast, euWest])
    await reportResult(db, { monitorId: id, owner, regionId: usEast, success: true, statusCode: 200, responseMs: 40 })
    await reportResult(db, { monitorId: id, owner, regionId: euWest, success: true, statusCode: 200, responseMs: 60 })
    const name = await renderEdit(id)

    expect(await screen.findByRole('button', { name: 'US East' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'EU West' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'AP South' })).toHaveAttribute('aria-pressed', 'false')

    await userEvent.clear(name)
    await userEvent.type(name, 'alpha-renamed')
    await userEvent.clear(screen.getByLabelText('Interval (seconds)'))
    await userEvent.type(screen.getByLabelText('Interval (seconds)'), '120')
    await userEvent.click(screen.getByRole('button', { name: 'EU West' }))
    await userEvent.click(screen.getByRole('button', { name: 'AP South' }))
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    await waitFor(() =>
      expect(navigate).toHaveBeenCalledWith({ to: '/monitors/$monitorId', params: { monitorId: id } }),
    )
    const [monitor] = await monitorInDb(db, id)
    expect(monitor.name).toBe('alpha-renamed')
    expect(Number(monitor.interval.milliseconds())).toBe(120_000)
    expect(Number(monitor.timeout.milliseconds())).toBe(10_000)
    expect(monitor.status).toBe('up')
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'up', [apSouth]: 'unknown' })
    expect(await resultRegionsInDb(db, id)).toEqual([usEast])
  })

  it('rolls the whole edit back and shows the RQL error when a region call fails', async () => {
    // The update and every region call must share one transaction, otherwise the rename and the EU removal would stick.
    const id = await createMonitor(db, identity, 'alpha', [usEast, euWest])
    const gone = await addRegion(db, 'Zanzibar')
    const name = await renderEdit(id)

    await userEvent.clear(name)
    await userEvent.type(name, 'alpha-renamed')
    await userEvent.click(screen.getByRole('button', { name: 'EU West' }))
    await userEvent.click(screen.getByRole('button', { name: 'Zanzibar' }))
    // Removed after ticking, so the form still sends the id and only the server can refuse it.
    await commandRoot(db, DELETE_REGION, { id: gone })
    await caughtUp(client)
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    expect(await screen.findByText(/unknown region/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect(await monitorInDb(db, id)).toMatchObject([{ name: 'alpha' }])
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'unknown', [euWest]: 'unknown' })
  })

  it.each([
    ['monitors', () => monitors, 'monitors subscription refused'],
    ['monitor regions', () => monitorRegions, 'monitor regions subscription refused'],
  ] as const)('says the %s failed to load instead of spinning forever', async (_, query, message) => {
    // A failed subscription never turns ready, so a page that only waits for ready would spin with nothing saying why.
    const id = await createMonitor(db, identity, 'alpha', [usEast])
    store = refusingStore(client, query(), null, new Error(message))
    routeParams.monitorId = id
    renderWithProviders(<MonitorEditPage />, store)
    await caughtUp(client)

    expect(await screen.findByText(`Failed to load monitor: ${message}`)).toBeInTheDocument()
    expect(screen.queryByLabelText('Name')).not.toBeInTheDocument()
  })

  it('opens the form only once the monitor regions have arrived, even when the monitor is already loaded', async () => {
    // A form opened before its regions hydrate starts with none ticked, so a save would remove every region and its results.
    const id = await createMonitor(db, identity, 'alpha', [usEast, euWest])
    let release: () => void = () => undefined
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const slowRegions: StoreClient = {
      ...client,
      batchSubscribe: async (subscriptions) => {
        if (subscriptions.some((subscription) => subscription.rql === monitorRegions.rql)) await gate
        return client.batchSubscribe(subscriptions)
      },
    }
    const gated = new Store(slowRegions, STORE_OPTIONS)
    const held = [
      gated.subscribe(monitors, null),
      gated.subscribe(regions, null),
    ]
    await caughtUp(client)
    expect(gated.getEntry(monitors, null).status).toBe('ready')
    routeParams.monitorId = id
    renderWithProviders(<MonitorEditPage />, gated)
    await caughtUp(client)

    expect(screen.queryByLabelText('Name')).not.toBeInTheDocument()

    release()
    expect(await screen.findByLabelText('Name')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'US East' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'EU West' })).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    await waitFor(() =>
      expect(navigate).toHaveBeenCalledWith({ to: '/monitors/$monitorId', params: { monitorId: id } }),
    )
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'unknown', [euWest]: 'unknown' })
    held.forEach((releaseHeld) => releaseHeld())
  })
})

describe('the at-least-one-region check', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let identity: string
  let usEast: string
  let euWest: string

  beforeEach(async () => {
    db = create()
    ;({ store, client, identity } = await bridgeStore(db, 'tester'))
    usEast = await regionNamed(db, 'US East')
    euWest = await regionNamed(db, 'EU West')
  })

  function wrapper({ children }: { children: ReactNode }) {
    return <StoreProvider store={store}>{children}</StoreProvider>
  }

  function input(regionIds: string[]): MonitorInput {
    return {
      name: 'alpha-renamed',
      kind: 'http',
      target: 'https://alpha.example.com/health',
      interval_ms: 120_000,
      timeout_ms: 10_000,
      http_method: 'GET',
      failure_threshold: 3,
      enabled: true,
      regions: regionIds,
    }
  }

  async function monitorCount(): Promise<number> {
    const rows = await queryRoot(db, monitorIds, null)
    return rows.length
  }

  it('refuses an update that removes every region and rolls the whole edit back', async () => {
    // A monitor left with no region is never checked again, so the update must fail as a whole instead of saving it dead.
    const id = await createMonitor(db, identity, 'alpha', [usEast, euWest])
    const { result } = renderHook(() => useUpdateMonitor(id), { wrapper })
    await caughtUp(client)

    await act(async () => {
      await expect(result.current.update(input([]))).rejects.toThrow(/a monitor must use at least one region/)
    })

    expect(vi.mocked(client.command).mock.lastCall?.[1].removed_region_ids.items.map(String).sort()).toEqual(
      [usEast, euWest].sort(),
    )
    expect(await monitorInDb(db, id)).toMatchObject([{ name: 'alpha' }])
    expect(await monitorRegionsInDb(db, id)).toEqual({ [usEast]: 'unknown', [euWest]: 'unknown' })
    expect(result.current.error?.message).toMatch(/a monitor must use at least one region/)
  })

  it('refuses a create without regions and writes no monitor', async () => {
    // A create committed before the region check would leave a monitor that no probe ever runs.
    const { result } = renderHook(() => useCreateMonitor(), { wrapper })

    await act(async () => {
      await expect(result.current.create(input([]))).rejects.toThrow(/a monitor must use at least one region/)
    })

    expect(await monitorCount()).toBe(0)
  })
})
