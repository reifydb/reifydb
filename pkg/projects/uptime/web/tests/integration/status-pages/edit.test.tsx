// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from 'react'
import { act, renderHook, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Int2Value, StoreProvider, Uuid7Value, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { useCreateStatusPage, useUpdateStatusPage } from '@/hooks/use-status-pages'
import { StatusPageEditPage } from '@/pages/status-pages/form.tsx'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { navigate } from '../../support/router-mock'
import { createMonitor } from '../../support/monitors'
import { createStatusPage, readMembers, readPages } from '../../support/status-pages'

const route = vi.hoisted(() => ({ pageId: '' }))

vi.mock('@tanstack/react-router', async () => ({
  ...(await import('../../support/router-mock')).routerMock(),
  useParams: () => route,
}))

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('edit status page flow', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let alpha: string
  let beta: string
  let gamma: string

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
    alpha = await createMonitor(client, 'alpha')
    beta = await createMonitor(client, 'beta')
    gamma = await createMonitor(client, 'gamma')
    navigate.mockClear()
  })

  async function caughtUp() {
    await act(async () => {
      // without this yield the drain is queued ahead of the store's batch flush and misses the hydration
      await Promise.resolve()
      await client.caughtUp()
    })
  }

  async function renderPage(pageId: string) {
    route.pageId = pageId
    renderWithProviders(<StatusPageEditPage />, store)
    await caughtUp()
    vi.mocked(client.command).mockClear()
  }

  it('pre-fills the form from the live rows, then saves the new title, slug and member order', async () => {
    // A form mounted before the member rows hydrate would start unticked and silently drop every member.
    const id = await createStatusPage(client, 'acme', 'Acme', [alpha, beta])
    await renderPage(id)

    expect(await screen.findByLabelText('Title')).toHaveValue('Acme')
    expect(screen.getByLabelText('Slug')).toHaveValue('acme')
    expect(screen.getByRole('checkbox', { name: /alpha/ })).toBeChecked()
    expect(screen.getByRole('checkbox', { name: /beta/ })).toBeChecked()
    expect(screen.getByRole('checkbox', { name: /gamma/ })).not.toBeChecked()

    await userEvent.clear(screen.getByLabelText('Title'))
    await userEvent.type(screen.getByLabelText('Title'), 'Acme Cloud')
    await userEvent.clear(screen.getByLabelText('Slug'))
    await userEvent.type(screen.getByLabelText('Slug'), 'acme-cloud')
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('checkbox', { name: /gamma/ }))
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/status-pages' }))
    expect(client.command).toHaveBeenCalledTimes(1)
    expect(await readPages(db)).toEqual([{ id, slug: 'acme-cloud', title: 'Acme Cloud' }])
    expect(await readMembers(db, id)).toEqual([
      { monitorId: beta, position: 0 },
      { monitorId: gamma, position: 1 },
    ])
  })

  it('keeps the member order when an untouched page is saved, whatever order the rows arrived in', async () => {
    // Members read in arrival order instead of by position would silently reorder the public page on save.
    const id = await createStatusPage(client, 'acme', 'Acme', [])
    const add = 'CALL uptime::add_status_page_monitor($id, $monitor_id, $position)'
    await client.command(add, { id, monitor_id: gamma, position: new Int2Value(1) }, [])
    await client.command(add, { id, monitor_id: alpha, position: new Int2Value(0) }, [])
    await client.command(add, { id, monitor_id: beta, position: new Int2Value(2) }, [])
    await renderPage(id)

    await userEvent.click(await screen.findByRole('button', { name: /save changes/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/status-pages' }))
    expect(await readMembers(db, id)).toEqual([
      { monitorId: alpha, position: 0 },
      { monitorId: gamma, position: 1 },
      { monitorId: beta, position: 2 },
    ])
  })

  it('shows the taken slug error from RQL and leaves the page as it was', async () => {
    // Slugs are unique per owner only in RQL, so a clash must surface in the form and change nothing.
    const id = await createStatusPage(client, 'acme', 'Acme', [alpha])
    await createStatusPage(client, 'other', 'Other', [beta])
    await renderPage(id)

    await userEvent.clear(await screen.findByLabelText('Slug'))
    await userEvent.type(screen.getByLabelText('Slug'), 'other')
    await userEvent.click(screen.getByRole('checkbox', { name: /gamma/ }))
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    expect(await screen.findByText(/this slug is already taken/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect((await readPages(db)).find((p) => p.id === id)).toEqual({ id, slug: 'acme', title: 'Acme' })
    expect(await readMembers(db, id)).toEqual([{ monitorId: alpha, position: 0 }])
  })

  it('keeps the old title and members when an add fails after the clear', async () => {
    // Update, clear and re-add in separate commands would commit the title and wipe the members when an add fails.
    const id = await createStatusPage(client, 'acme', 'Acme', [alpha, beta])
    await renderPage(id)

    await userEvent.clear(await screen.findByLabelText('Title'))
    await userEvent.type(screen.getByLabelText('Title'), 'Acme Cloud')
    await userEvent.click(screen.getByRole('checkbox', { name: /gamma/ }))
    await userEvent.click(screen.getByRole('checkbox', { name: /alpha/ }))
    await userEvent.click(screen.getByRole('checkbox', { name: /beta/ }))
    await client.command('CALL uptime::delete_monitor($id)', { id: new Uuid7Value(gamma) }, [])
    await caughtUp()
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    expect(await screen.findByText(/unknown monitor id/)).toBeInTheDocument()
    expect(navigate).not.toHaveBeenCalled()
    expect(await readPages(db)).toEqual([{ id, slug: 'acme', title: 'Acme' }])
    expect(await readMembers(db, id)).toEqual([
      { monitorId: alpha, position: 0 },
      { monitorId: beta, position: 1 },
    ])
  })

  it('lists a ticked monitor deleted elsewhere as a removable row, so the page can still be saved', async () => {
    // A ticked monitor deleted elsewhere has no checkbox left; without a row to remove it, every save fails on an unknown monitor id.
    const id = await createStatusPage(client, 'acme', 'Acme', [alpha, beta])
    await renderPage(id)
    expect(await screen.findByRole('checkbox', { name: /beta/ })).toBeChecked()

    await client.command('CALL uptime::delete_monitor($id)', { id: new Uuid7Value(beta) }, [])
    await caughtUp()

    expect(screen.queryByRole('checkbox', { name: /beta/ })).not.toBeInTheDocument()
    expect(screen.getByText('Deleted monitor')).toBeInTheDocument()
    expect(screen.getByText(beta)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Remove' }))
    expect(screen.queryByText('Deleted monitor')).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: /save changes/i }))

    await waitFor(() => expect(navigate).toHaveBeenCalledWith({ to: '/status-pages' }))
    expect(await readMembers(db, id)).toEqual([{ monitorId: alpha, position: 0 }])
  })

  it("shows not found for another owner's page id and never writes to it", async () => {
    // The from-policy is the only thing keeping a guessed page id from opening someone else's page.
    const { client: other } = await bridgeStore(db, 'mallory')
    const theirs = await createMonitor(other, 'theirs')
    const id = await createStatusPage(other, 'mallory', 'Mallory', [theirs])
    await renderPage(id)

    expect(await screen.findByText('Status page not found')).toBeInTheDocument()
    expect(screen.queryByLabelText('Title')).not.toBeInTheDocument()
    expect(client.command).not.toHaveBeenCalled()
    expect(await readMembers(db, id)).toEqual([{ monitorId: theirs, position: 0 }])
  })
})

describe('the at-least-one-monitor check', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient
  let alpha: string
  let beta: string

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
    alpha = await createMonitor(client, 'alpha')
    beta = await createMonitor(client, 'beta')
  })

  function wrapper({ children }: { children: ReactNode }) {
    return <StoreProvider store={store}>{children}</StoreProvider>
  }

  it('refuses an update that leaves no member and rolls the clear and the rename back', async () => {
    // The update clears every member before re-adding; with nothing re-added the page would publish an empty status.
    const id = await createStatusPage(client, 'acme', 'Acme', [alpha, beta])
    const { result } = renderHook(() => useUpdateStatusPage(id), { wrapper })

    await act(async () => {
      await expect(result.current.update({ slug: 'acme-cloud', title: 'Acme Cloud', monitor_ids: [] })).rejects.toThrow(
        /a status page must contain at least one monitor/,
      )
    })

    expect(await readPages(db)).toEqual([{ id, slug: 'acme', title: 'Acme' }])
    expect(await readMembers(db, id)).toEqual([
      { monitorId: alpha, position: 0 },
      { monitorId: beta, position: 1 },
    ])
    expect(result.current.error?.message).toMatch(/a status page must contain at least one monitor/)
  })

  it('refuses a create without members and writes no page', async () => {
    // A create committed before the member check would publish a status page that shows nothing.
    const { result } = renderHook(() => useCreateStatusPage(), { wrapper })

    await act(async () => {
      await expect(result.current.create({ slug: 'acme', title: 'Acme', monitor_ids: [] })).rejects.toThrow(
        /a status page must contain at least one monitor/,
      )
    })

    expect(await readPages(db)).toEqual([])
  })
})
