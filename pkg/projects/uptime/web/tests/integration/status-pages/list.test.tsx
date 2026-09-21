// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { ListValue, Uuid7Value, type Store } from '@reifydb/react'
import type { BridgeClient, TestDb, TestFactory } from '@reifydb/reifydb'
import { StatusPagesPage } from '@/pages/status-pages'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { createMonitor } from '../../support/monitors'
import { createStatusPage, readMembers, readPages } from '../../support/status-pages'

vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

let create: TestFactory

beforeAll(() => {
  create = loadBackend()
})

describe('status pages list over a live subscription', () => {
  let db: TestDb
  let store: Store
  let client: BridgeClient

  beforeEach(async () => {
    db = create()
    ;({ store, client } = await bridgeStore(db, 'tester'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  async function caughtUp() {
    await act(async () => {
      // without this yield the drain is queued ahead of the store's batch flush and misses the hydration
      await Promise.resolve()
      await client.caughtUp()
    })
  }

  it('hydrates the pages that already exist, with their member counts', async () => {
    // Without hydration an account that already has pages would show the empty state until the next write.
    const alpha = await createMonitor(client, 'alpha')
    const beta = await createMonitor(client, 'beta')
    await createStatusPage(client, 'acme', 'Acme status', [alpha, beta])

    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()

    const row = await screen.findByRole('row', { name: /Acme status/ })
    expect(within(row).getByRole('link', { name: '/status/acme' })).toHaveAttribute('href', '/status/acme')
    expect(within(row).getByText('2')).toBeInTheDocument()
  })

  it('shows a page created by another path after the list mounted, without a refetch', async () => {
    // A write must reach an already-mounted list as a change; nothing on the page asks again.
    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()
    expect(await screen.findByRole('heading', { name: 'No status pages yet' })).toBeInTheDocument()

    const alpha = await createMonitor(client, 'alpha')
    await createStatusPage(client, 'late', 'Late page', [alpha])
    await caughtUp()

    const row = await screen.findByRole('row', { name: /Late page/ })
    expect(within(row).getByText('1')).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'No status pages yet' })).not.toBeInTheDocument()
  })

  it('updates the member count live when another path adds a monitor to a mounted page', async () => {
    // The count is composed from a second subscription, so a change there alone must re-render the row.
    const alpha = await createMonitor(client, 'alpha')
    const beta = await createMonitor(client, 'beta')
    const id = await createStatusPage(client, 'acme', 'Acme status', [alpha])
    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()
    expect(within(await screen.findByRole('row', { name: /Acme status/ })).getByText('1')).toBeInTheDocument()

    await client.command(
      'CALL uptime::add_status_page_monitors($id, $monitor_ids)',
      { id, monitor_ids: new ListValue([new Uuid7Value(beta)], 'Uuid7') },
      [],
    )
    await caughtUp()

    await waitFor(() =>
      expect(within(screen.getByRole('row', { name: /Acme status/ })).getByText('2')).toBeInTheDocument(),
    )
  })

  it("never shows another owner's pages, neither hydrated nor written after mount", async () => {
    // The from-policy must scope both hydration and live diffs, or one user's list leaks every tenant.
    const { client: other } = await bridgeStore(db, 'mallory')
    const theirs = await createMonitor(other, 'theirs')
    await createStatusPage(other, 'before', 'Mallory before', [theirs])

    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()
    await createStatusPage(other, 'after', 'Mallory after', [theirs])
    const mine = await createMonitor(client, 'mine')
    await createStatusPage(client, 'mine', 'My page', [mine])
    await caughtUp()

    expect(await screen.findByRole('row', { name: /My page/ })).toBeInTheDocument()
    expect(screen.queryByText(/Mallory/)).not.toBeInTheDocument()
    expect((await readPages(db)).map((p) => p.title)).toEqual(['Mallory after', 'Mallory before', 'My page'])
  })

  it('deletes a confirmed page with its members and the row leaves the list live', async () => {
    // Members left behind would still render on the public page; the sibling page must survive untouched.
    const alpha = await createMonitor(client, 'alpha')
    const keep = await createStatusPage(client, 'keep', 'Keep me', [alpha])
    const drop = await createStatusPage(client, 'drop', 'Drop me', [alpha])
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()
    vi.mocked(client.command).mockClear()

    const row = await screen.findByRole('row', { name: /Drop me/ })
    await userEvent.click(within(row).getByRole('button', { name: 'Delete status page' }))
    await Promise.all(vi.mocked(client.command).mock.results.map((r) => r.value))
    await caughtUp()

    await waitFor(() => expect(screen.queryByRole('row', { name: /Drop me/ })).not.toBeInTheDocument())
    expect(screen.getByRole('row', { name: /Keep me/ })).toBeInTheDocument()
    expect(await readPages(db)).toEqual([{ id: keep, slug: 'keep', title: 'Keep me' }])
    expect(await readMembers(db, drop)).toEqual([])
    expect(await readMembers(db, keep)).toEqual([{ monitorId: alpha, position: 0 }])
  })

  it('writes nothing when the delete confirmation is dismissed', async () => {
    // A dismissed confirm that still deleted would lose a page with no undo.
    const alpha = await createMonitor(client, 'alpha')
    const id = await createStatusPage(client, 'acme', 'Acme status', [alpha])
    vi.spyOn(window, 'confirm').mockReturnValue(false)
    renderWithProviders(<StatusPagesPage />, store)
    await caughtUp()
    vi.mocked(client.command).mockClear()

    const row = await screen.findByRole('row', { name: /Acme status/ })
    await userEvent.click(within(row).getByRole('button', { name: 'Delete status page' }))

    expect(client.command).not.toHaveBeenCalled()
    expect(await readPages(db)).toEqual([{ id, slug: 'acme', title: 'Acme status' }])
  })
})
