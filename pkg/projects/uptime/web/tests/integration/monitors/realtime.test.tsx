// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The realtime path, end to end and in-process.
//
// Every other test in this directory seeds the store by hand, so nothing has ever exercised what
// the dashboard actually runs on: a `useSubscription` that hydrates from the real schema and then
// keeps receiving changes. Here the rows arrive the way they do in production, over the same
// encoded envelopes the WebSocket transport sends, with only the transport swapped for the native
// bridge.

import { act, screen, waitFor, within } from '@testing-library/react'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { DurationValue, Option, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { DashboardPage } from '@/pages/dashboard'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

const CREATE_MONITOR =
  'CALL uptime::create_monitor($name, $kind, $target, $interval, $timeout, $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)'

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

// Batching is what uptime ships, so it is what the batched block below inherits. This block pins
// the other path: a transport with no batchSubscribe still has to drive the same page.
describe('the dashboard over a live subscription, unbatched', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester', { batch: false }))
  })

  function createMonitor(name: string, target: string) {
    return client.command(
      CREATE_MONITOR,
      {
        name,
        kind: 'http',
        target,
        interval: DurationValue.fromMilliseconds(60_000),
        timeout: DurationValue.fromMilliseconds(10_000),
        http_method: Option.none('Utf8'),
        expected_status: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expected_ip: Option.none('Utf8'),
        failure_threshold: 3,
        enabled: true,
      },
      [],
    )
  }

  // Inside act so the subscription callbacks reach React state under the test's control.
  async function caughtUp() {
    await act(async () => {
      await client.caughtUp()
    })
  }

  it('hydrates the monitors that already exist when the page mounts', async () => {
    // The dashboard is normally opened on an account that already has monitors. Without hydration
    // it would render its empty state and stay there until the next write happened to arrive.
    await createMonitor('alpha-api', 'https://alpha.example.com/health')
    await caughtUp()

    renderWithProviders(<DashboardPage />, store)

    const row = await screen.findByRole('row', { name: /alpha-api/i })
    expect(within(row).getByText('https://alpha.example.com/health')).toBeInTheDocument()
  })

  it('shows a monitor created after the page mounted, without a refetch', async () => {
    // This is the claim the whole realtime path makes: a write reaches an already-mounted page as
    // a change, not because anything asked again. A test that seeded the store could never tell
    // the two apart.
    renderWithProviders(<DashboardPage />, store)
    await caughtUp()
    expect(await screen.findByRole('heading', { name: 'No monitors yet' })).toBeInTheDocument()

    await createMonitor('beta-api', 'https://beta.example.com/health')
    await caughtUp()

    await waitFor(() => expect(screen.getByRole('row', { name: /beta-api/i })).toBeInTheDocument())
    expect(screen.queryByRole('heading', { name: 'No monitors yet' })).not.toBeInTheDocument()
  })
})

describe('the dashboard over one batched subscription', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    // No override: this is uptime's own STORE_OPTIONS, so the assertions below fail if production
    // ever stops batching.
    ;({ store, client } = await bridgeStore(db, 'tester'))
  })

  async function caughtUp() {
    await act(async () => {
      await client.caughtUp()
    })
  }

  it('opens every subscription the page needs as one batch', async () => {
    // An empty dashboard mounts five distinct subscriptions. The sixth, uptime::results, belongs to
    // a monitor row and opens in a later tick once there is a row to mount it. Batched, the page
    // costs one round trip instead of five, and no hook had to be told that is what happened.
    renderWithProviders(<DashboardPage />, store)
    await caughtUp()

    expect(client.subscribe).not.toHaveBeenCalled()
    expect(client.batchSubscribe).toHaveBeenCalledTimes(1)
    const members = (client.batchSubscribe as unknown as ReturnType<typeof vi.fn>).mock.calls[0][0]
    expect(members.map((member: { rql: string }) => member.rql).sort()).toEqual([
      'from uptime::daily_totals',
      'from uptime::daily_ups',
      'from uptime::monitor_regions',
      'from uptime::monitors',
      'from uptime::regions',
    ])
  })

  it('hydrates the page from a batch, then keeps it live', async () => {
    // Batching must not cost the page anything it had before. Hydration rides the batch envelope
    // rather than a plain change, and forward changes have to keep arriving afterwards.
    await client.command(
      CREATE_MONITOR,
      {
        name: 'gamma-api',
        kind: 'http',
        target: 'https://gamma.example.com/health',
        interval: DurationValue.fromMilliseconds(60_000),
        timeout: DurationValue.fromMilliseconds(10_000),
        http_method: Option.none('Utf8'),
        expected_status: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expected_ip: Option.none('Utf8'),
        failure_threshold: 3,
        enabled: true,
      },
      [],
    )

    renderWithProviders(<DashboardPage />, store)
    await caughtUp()
    expect(await screen.findByRole('row', { name: /gamma-api/i })).toBeInTheDocument()

    await client.command(
      CREATE_MONITOR,
      {
        name: 'delta-api',
        kind: 'http',
        target: 'https://delta.example.com/health',
        interval: DurationValue.fromMilliseconds(60_000),
        timeout: DurationValue.fromMilliseconds(10_000),
        http_method: Option.none('Utf8'),
        expected_status: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expected_ip: Option.none('Utf8'),
        failure_threshold: 3,
        enabled: true,
      },
      [],
    )
    await caughtUp()

    await waitFor(() => expect(screen.getByRole('row', { name: /delta-api/i })).toBeInTheDocument())
  })
})
