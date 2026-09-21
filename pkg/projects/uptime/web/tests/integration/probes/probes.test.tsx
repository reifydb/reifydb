// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { IdentityIdValue, StoreProvider, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { probes, useProbeNames, useProbes } from '@/hooks/use-probes'
import { ProbesPage } from '@/pages/probes'
import { loadBackend } from '../../support/backend'
import { identityNamed } from '../../support/identity'
import { bridgeStore, refusingStore, renderWithProviders } from '../../support/store'

const MINUTE = 60_000

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

async function createProbeService(db: TestDb, name: string): Promise<string> {
  await db.adminRoot(`CREATE SERVICE ${name}`, {}, [])
  return identityNamed(db, name)
}

function registerProbe(db: TestDb, probe: string, name: string, seen: Date) {
  return db.commandAs(
    probe,
    'CALL uptime::register_probe($probe, $name, $seen)',
    { probe: new IdentityIdValue(probe), name, seen },
    [],
  )
}

function heartbeat(db: TestDb, probe: string, seen: Date) {
  return db.commandAs(
    probe,
    'CALL uptime::probe_heartbeat($probe, $seen)',
    { probe: new IdentityIdValue(probe), seen },
    [],
  )
}

function ProbeNames() {
  const { data } = useProbes()
  const { data: names } = useProbeNames()
  return (
    <>
      {data != null && <p>probes ready</p>}
      <ul>
        {Object.entries(names).map(([id, name]) => (
          <li key={id}>{`${id} ${name}`}</li>
        ))}
      </ul>
    </>
  )
}

describe('the probes page over a live subscription', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'viewer'))
  })

  async function caughtUp() {
    await act(async () => {
      await client.caughtUp()
    })
  }

  it('hydrates the probes registered before the page mounted, sorted by name', async () => {
    // The page is usually opened long after the probes registered; without hydration it would show none until one heartbeats.
    const us = await createProbeService(db, 'probe_us')
    const eu = await createProbeService(db, 'probe_eu')
    await registerProbe(db, us, 'us-east', new Date())
    await registerProbe(db, eu, 'eu-west', new Date())

    renderWithProviders(<ProbesPage />, store)
    await caughtUp()

    const rows = await screen.findAllByRole('row', { name: /-(east|west)/ })
    expect(rows.map((row) => within(row).getAllByRole('cell')[0].textContent)).toEqual(['eu-west', 'us-east'])
    expect(within(rows[0]).getByText(eu)).toBeInTheDocument()
    expect(within(rows[0]).getByText('Online')).toBeInTheDocument()
  })

  it('shows a probe registered after the page mounted, without a reload', async () => {
    // Probes register while the page is open; the subscription must deliver the row, since nothing polls any more.
    const probe = await createProbeService(db, 'probe_ap')
    renderWithProviders(<ProbesPage />, store)
    await caughtUp()
    expect(await screen.findByText('No probes registered')).toBeInTheDocument()

    await registerProbe(db, probe, 'ap-south', new Date())
    await caughtUp()

    const row = await screen.findByRole('row', { name: /ap-south/ })
    expect(within(row).getByText('Online')).toBeInTheDocument()
    expect(screen.queryByText('No probes registered')).not.toBeInTheDocument()
  })

  it('flips a silent probe to online when its heartbeat arrives', async () => {
    // A heartbeat is an update to an existing row; a store that only merged inserts would leave the probe offline.
    const probe = await createProbeService(db, 'probe_sa')
    await registerProbe(db, probe, 'sa-east', new Date(Date.now() - 5 * MINUTE))
    renderWithProviders(<ProbesPage />, store)
    await caughtUp()
    const row = await screen.findByRole('row', { name: /sa-east/ })
    expect(within(row).getByText('Offline')).toBeInTheDocument()

    await heartbeat(db, probe, new Date())
    await caughtUp()

    await waitFor(() =>
      expect(within(screen.getByRole('row', { name: /sa-east/ })).getByText('Online')).toBeInTheDocument(),
    )
    expect(screen.getAllByRole('row', { name: /sa-east/ })).toHaveLength(1)
  })

  it('names a probe as soon as it registers', async () => {
    // Result rows show the probe by name; a names map read once would leave a newly registered probe as a bare id.
    const probe = await createProbeService(db, 'probe_af')
    render(
      <StoreProvider store={store}>
        <ProbeNames />
      </StoreProvider>,
    )
    await caughtUp()
    // Registering before the subscription hydrates would let hydration, not the live diff, deliver the name.
    expect(await screen.findByText('probes ready')).toBeInTheDocument()
    expect(screen.queryByText(`${probe} af-south`)).not.toBeInTheDocument()

    await registerProbe(db, probe, 'af-south', new Date())
    await caughtUp()

    expect(await screen.findByText(`${probe} af-south`)).toBeInTheDocument()
  })
})

describe('the probes page without new data', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'viewer'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  async function caughtUp() {
    await act(async () => {
      // without this yield the drain is queued ahead of the store's batch flush and misses the hydration
      await Promise.resolve()
      await client.caughtUp()
    })
  }

  it('marks a probe offline once its last heartbeat ages past the window', async () => {
    // Nothing polls and a dead probe sends nothing, so only the clock can flip it; otherwise it stays online forever.
    const probe = await createProbeService(db, 'probe_eu')
    await registerProbe(db, probe, 'eu-west', new Date('2026-09-11T11:59:50Z'))
    // Only the page clock is faked; the bridge and the queries wait on real timeouts.
    vi.useFakeTimers({ now: new Date('2026-09-11T12:00:00Z'), toFake: ['Date', 'setInterval', 'clearInterval'] })
    renderWithProviders(<ProbesPage />, store)
    await caughtUp()
    expect(within(screen.getByRole('row', { name: /eu-west/ })).getByText('Online')).toBeInTheDocument()

    act(() => {
      vi.advanceTimersByTime(25_000)
    })

    expect(within(screen.getByRole('row', { name: /eu-west/ })).getByText('Offline')).toBeInTheDocument()
  })

  it('shows why the probes failed to load', async () => {
    // A failed subscription must say so; an empty table would claim no probes are registered.
    store = refusingStore(client, probes, null, new Error('policy denied'))

    renderWithProviders(<ProbesPage />, store)
    await caughtUp()

    expect(screen.getByText('Failed to load probes: policy denied')).toBeInTheDocument()
    expect(screen.queryByText('No probes registered')).not.toBeInTheDocument()
  })
})
