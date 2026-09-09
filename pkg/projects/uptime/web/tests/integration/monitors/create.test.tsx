// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { MonitorNewPage } from '@/pages/monitors/new.tsx'
import { regions } from '@/store/queries'
import type { TestDb, TestFactory } from '@reifydb/reifydb'
import { Utf8Value } from '@reifydb/core'
import { Option, Shape, type Store, type StoreClient } from '@reifydb/react'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'

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
    db = create(1)
    ;({ store, client } = await bridgeStore(db, 'tester'))
    store.seed(regions.rql, null, regions.shape, [{ id: 'region-1', label: 'US East' }])
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
        httpMethod: Option.none('Utf8'),
        expectedStatus: Option.none('Int2'),
        keyword: Option.none('Utf8'),
        expectedIp: Option.some('93.184.216.34'),
      },
    ])
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
