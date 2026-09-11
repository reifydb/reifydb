// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Uuid7Value, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { StatusPageNewPage } from '@/pages/status-pages/form.tsx'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'
import { createMonitor } from '../../support/monitors'
import { countMembers, createStatusPage, readMembers, readPages } from '../../support/status-pages'

vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('create status page flow', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
    navigate.mockClear()
  })

  async function caughtUp() {
    await act(async () => {
      // without this yield the drain is queued ahead of the store's batch flush and misses the hydration
      await Promise.resolve()
      await client.caughtUp()
    })
  }

  async function renderPage() {
    renderWithProviders(<StatusPageNewPage />, store)
    await caughtUp()
    vi.mocked(client.command).mockClear()
  }

  it('saves the page and its monitors in the order they were ticked, in one command', async () => {
    // Ticking order is the display order on the public page; one command keeps page and members atomic (F1).
    const alpha = await createMonitor(client, 'alpha')
    await createMonitor(client, 'beta')
    const gamma = await createMonitor(client, 'gamma')
    await renderPage()

    await userEvent.type(screen.getByLabelText('Title'), 'Acme Status')
    await userEvent.click(screen.getByRole('checkbox', { name: /gamma/ }))
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('button', { name: /create status page/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/status-pages' }))
    expect(client.command).toHaveBeenCalledTimes(1)
    const pages = await readPages(db)
    expect(pages).toEqual([{ id: expect.any(String), slug: 'acme-status', title: 'Acme Status' }])
    expect(await readMembers(db, pages[0].id)).toEqual([
      { monitorId: gamma, position: 0 },
      { monitorId: alpha, position: 1 },
    ])
  })

  it('shows the taken slug error from RQL in the form and writes nothing', async () => {
    // The uniqueness check lives only in RQL now, so its message is the only thing telling the user why.
    const alpha = await createMonitor(client, 'alpha')
    await createStatusPage(client, 'acme', 'Existing', [alpha])
    await renderPage()

    await userEvent.type(screen.getByLabelText('Title'), 'Another')
    await userEvent.clear(screen.getByLabelText('Slug'))
    await userEvent.type(screen.getByLabelText('Slug'), 'acme')
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('button', { name: /create status page/i }))

    expect(await screen.findByText(/this slug is already taken/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect((await readPages(db)).map((p) => p.title)).toEqual(['Existing'])
    expect(await countMembers(db)).toBe(1)
  })

  it('rolls the page back when a ticked monitor was deleted before submit', async () => {
    // A page committed without its failing member would publish an incomplete status page.
    await createMonitor(client, 'alpha')
    const beta = await createMonitor(client, 'beta')
    await renderPage()

    await userEvent.type(screen.getByLabelText('Title'), 'Acme Status')
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('checkbox', { name: /beta/ }))
    await client.command('CALL uptime::delete_monitor($id)', { id: new Uuid7Value(beta) }, [])
    await caughtUp()
    expect(screen.queryByRole('checkbox', { name: /beta/ })).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: /create status page/i }))

    expect(await screen.findByText(/unknown monitor id/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect(await readPages(db)).toEqual([])
    expect(await countMembers(db)).toBe(0)
  })

  it('stores a uuid-shaped title exactly as typed', async () => {
    // A bare string that looks like a uuid is sent as a uuid7, and the engine would store its normalised text.
    await createMonitor(client, 'alpha')
    const uuid = Uuid7Value.generate().toString()
    await renderPage()

    await userEvent.type(screen.getByLabelText('Title'), uuid.toUpperCase())
    await userEvent.clear(screen.getByLabelText('Slug'))
    await userEvent.type(screen.getByLabelText('Slug'), uuid)
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('button', { name: /create status page/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/status-pages' }))
    expect(await readPages(db)).toEqual([{ id: expect.any(String), slug: uuid, title: uuid.toUpperCase() }])
  })

  it('blocks a submit with no monitor ticked, never touching the database', async () => {
    // "At least one monitor" is a form-only rule, so nothing else would stop an empty page.
    await createMonitor(client, 'alpha')
    await renderPage()

    await userEvent.type(screen.getByLabelText('Title'), 'Acme Status')
    await userEvent.click(screen.getByRole('button', { name: /create status page/i }))

    expect(await screen.findByText('Select at least one monitor')).toBeInTheDocument()
    expect(client.command).not.toHaveBeenCalled()
    expect(await readPages(db)).toEqual([])
  })
})
