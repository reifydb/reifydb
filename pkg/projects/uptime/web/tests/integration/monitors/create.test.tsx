// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { MonitorNewPage } from '@/pages/monitors/new.tsx'
import { regions } from '@/store/queries'
import type { TestDb, TestFactory } from '@reifydb/reifydb'
import { Utf8Value } from '@reifydb/core'
import { Option, Shape, Uuid7Value, type Store, type StoreClient } from '@reifydb/react'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'
import { monitorInDb, monitorRegionsInDb, realRegions, regionNamed } from '../../support/monitors'

// a dynamic import inside the factory runs lazily; a static one is hoisted above this call and throws TDZ
vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let create: TestFactory

const OPTIONALS_RQL =
  'from uptime::monitors filter { name == $name } map { http_method, expected_status, keyword, expected_ip }'

const OPTIONALS = Shape.object({
  http_method: Shape.option(Shape.string()),
  expected_status: Shape.option(Shape.int2()),
  keyword: Shape.option(Shape.string()),
  expected_ip: Shape.option(Shape.string()),
})

beforeAll(() => {
  create = loadBackend()
})

describe('create monitor flow', () => {
  let db: TestDb
  let store: Store
  let client: StoreClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
    store.seed(regions.rql, null, regions.shape, await realRegions(db))
    navigate.mockClear()
  })

  function renderPage() {
    return renderWithProviders(<MonitorNewPage />, store)
  }

  it('creates a monitor through the real form and it lands in the real uptime schema', async () => {
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'reifydb.com')
    await userEvent.type(screen.getByLabelText('URL'), 'https://reifydb.com/health')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [{ params }] = navigate.mock.calls[0]
    expect(params.monitorId).toBeTruthy()

    // Asserts against the real db, not the mock response - otherwise a broken migration would go undetected.
    const [rows] = await db.queryRoot(
      'from uptime::monitors filter { name == "reifydb.com" } map { name, kind, target, status }',
      {},
      [
        Shape.object({
          name: Shape.utf8Value(),
          kind: Shape.utf8Value(),
          target: Shape.utf8Value(),
          status: Shape.utf8Value(),
        }),
      ],
    )
    expect(rows).toEqual([
      {
        '#rownum': 1,
        name: new Utf8Value('reifydb.com'),
        kind: new Utf8Value('http'),
        target: new Utf8Value('https://reifydb.com/health'),
        status: new Utf8Value('unknown'),
      },
    ])
  })

  it('stores a none in every optional column when the form submits none of them', async () => {
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'db-port')
    await userEvent.selectOptions(screen.getByLabelText('Type'), 'tcp')
    await userEvent.type(screen.getByLabelText('Host and port'), 'db.example.com:5432')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [rows] = await db.queryRoot(OPTIONALS_RQL, { name: 'db-port' }, [OPTIONALS])
    expect(rows).toEqual([
      {
        '#rownum': 1,
        httpMethod: Option.none('Utf8'),
        expectedStatus: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expectedIp: Option.none('Utf8'),
      },
    ])
  })

  it('stores the http method, expected status and keyword the form submits for an http monitor', async () => {
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'api-health')
    await userEvent.type(screen.getByLabelText('URL'), 'https://api.example.com/health')
    await userEvent.selectOptions(screen.getByLabelText('HTTP method'), 'HEAD')
    await userEvent.type(screen.getByLabelText('Expected status code (empty = any 2xx)'), '204')
    await userEvent.type(screen.getByLabelText('Response keyword (optional)'), 'ok')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [rows] = await db.queryRoot(OPTIONALS_RQL, { name: 'api-health' }, [OPTIONALS])
    expect(rows).toEqual([
      {
        '#rownum': 1,
        httpMethod: Option.some('HEAD'),
        expectedStatus: Option.some(204),
        keyword: Option.some('ok'),
        expectedIp: Option.none('Utf8'),
      },
    ])
  })

  it('stores the expected ip the form submits for a dns monitor', async () => {
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'dns-check')
    await userEvent.selectOptions(screen.getByLabelText('Type'), 'dns')
    await userEvent.type(screen.getByLabelText('Hostname'), 'example.com')
    await userEvent.type(screen.getByLabelText('Expected IP (optional)'), '93.184.216.34')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [rows] = await db.queryRoot(OPTIONALS_RQL, { name: 'dns-check' }, [OPTIONALS])
    expect(rows).toEqual([
      {
        '#rownum': 1,
        httpMethod: Option.none('Utf8'),
        expectedStatus: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expectedIp: Option.some('93.184.216.34'),
      },
    ])
  })

  it('stores a name and keyword that look like a uuid exactly as typed', async () => {
    // A uuid-shaped string sent without a type goes over as a Uuid7 and comes back lowercased.
    const uuidLike = 'ABCDEF01-2345-7678-89AB-CDEF01234567'
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), uuidLike)
    await userEvent.type(screen.getByLabelText('URL'), 'https://uuid.example.com/health')
    await userEvent.type(screen.getByLabelText('Response keyword (optional)'), uuidLike)
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [{ params }] = navigate.mock.calls[0]
    const [rows] = await db.queryRoot(
      'from uptime::monitors filter { id == $id } map { name, keyword }',
      { id: params.monitorId },
      [Shape.object({ name: Shape.utf8(), keyword: Shape.option(Shape.utf8()) })],
    )
    expect(rows).toEqual([{ '#rownum': 1, name: uuidLike, keyword: Option.some(uuidLike) }])
  })

  it('saves every selected region with the monitor, under the id the page navigates to', async () => {
    // A monitor saved without its regions is never checked by any probe.
    const usEast = await regionNamed(db, 'US East')
    const euWest = await regionNamed(db, 'EU West')
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'multi-region')
    await userEvent.type(screen.getByLabelText('URL'), 'https://multi.example.com/health')
    await userEvent.click(screen.getByRole('button', { name: 'US East' }))
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalled())
    const [{ params }] = navigate.mock.calls[0]
    expect(await monitorInDb(db, params.monitorId)).toMatchObject([{ name: 'multi-region' }])
    expect(await monitorRegionsInDb(db, params.monitorId)).toEqual({
      [usEast]: 'unknown',
      [euWest]: 'unknown',
    })
  })

  it('rolls the monitor back and shows the RQL error when a selected region no longer exists', async () => {
    // create_monitor and every add_monitor_region must share one transaction, otherwise a half-saved monitor remains.
    const gone = Uuid7Value.generate().toString()
    store.seed(regions.rql, null, regions.shape, [...(await realRegions(db)), { id: gone, label: 'Zanzibar' }])
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'reifydb.com')
    await userEvent.type(screen.getByLabelText('URL'), 'https://reifydb.com/health')
    await userEvent.click(screen.getByRole('button', { name: 'Zanzibar' }))
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    expect(await screen.findByText(/unknown region/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    const [monitorRows] = await db.queryRoot('from uptime::monitors map { name }', {}, [
      Shape.object({ name: Shape.utf8() }),
    ])
    expect(monitorRows).toEqual([])
    const [regionRows] = await db.queryRoot('from uptime::monitor_regions map { region_id }', {}, [
      Shape.object({ region_id: Shape.uuid7() }),
    ])
    expect(regionRows).toEqual([])
  })

  it('blocks submission client-side when no region is selected, never touching the network', async () => {
    store.seed(regions.rql, null, regions.shape, [])
    renderPage()

    await userEvent.type(screen.getByLabelText('Name'), 'reifydb.com')
    await userEvent.type(screen.getByLabelText('URL'), 'https://reifydb.com')
    await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))

    expect(await screen.findByText(/select at least one region/i)).toBeInTheDocument()
    expect(client.command).not.toHaveBeenCalled()
    expect(navigate).not.toHaveBeenCalled()
  })
})
