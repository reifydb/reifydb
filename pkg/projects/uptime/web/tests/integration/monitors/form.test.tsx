// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Store } from '@reifydb/react'
import type { BridgeClient, TestFactory } from '@reifydb/reifydb'
import { MonitorNewPage } from '@/pages/monitors/new.tsx'
import { regions } from '@/store/queries'
import { loadBackend } from '../../support/backend'
import { bridgeStore, renderWithProviders } from '../../support/store'
import { caughtUp } from '../../support/monitors'
import { navigate } from '../../support/router-mock'

vi.mock('@reifydb/auth', async () => (await import('../../support/auth-mock')).authMock())
vi.mock('@tanstack/react-router', async () => (await import('../../support/router-mock')).routerMock())

const TARGET_LABEL = { http: 'URL', tcp: 'Host and port', ping: 'Host', dns: 'Hostname' } as const

type Kind = keyof typeof TARGET_LABEL

let create: TestFactory
let client: BridgeClient

beforeAll(() => {
  create = loadBackend()
})

async function renderForm() {
  let store
  ;({ store, client } = await bridgeStore(create(), 'tester'))
  renderWithProviders(<MonitorNewPage />, store)
  await caughtUp(client)
}

async function fillAndSubmit(kind: Kind, target: string) {
  await userEvent.type(screen.getByLabelText('Name'), 'check')
  if (kind !== 'http') await userEvent.selectOptions(screen.getByLabelText('Type'), kind)
  await userEvent.click(screen.getByLabelText(TARGET_LABEL[kind]))
  await userEvent.paste(target)
  await userEvent.click(screen.getByRole('button', { name: /create monitor/i }))
}

async function submit(kind: Kind, target: string) {
  await renderForm()
  await fillAndSubmit(kind, target)
}

describe('monitor form target checks', () => {
  beforeEach(() => {
    navigate.mockClear()
  })

  it.each([
    ['http', 'https://exa mple.com', 'URL is not valid'],
    ['http', 'https://', 'URL is not valid'],
    ['tcp', ':5432', 'TCP target must be host:port'],
    ['tcp', 'db.example.com:0', 'TCP port must be between 1 and 65535'],
    ['tcp', 'db.example.com:70000', 'TCP port must be between 1 and 65535'],
    ['ping', 'https://example.com', 'Target must be a plain hostname'],
    ['ping', 'exa mple.com', 'Target must be a plain hostname'],
    ['dns', 'example.com/path', 'Target must be a plain hostname'],
    ['dns', 'example.com:53', 'Target must be a plain hostname'],
  ] as const)('refuses a %s target of %j before it reaches the server', async (kind, target, message) => {
    // The server no longer checks targets, so a malformed one the form lets through is stored and fails every check forever.
    await submit(kind, target)

    expect(await screen.findByText(message)).toBeInTheDocument()
    expect(client.command).not.toHaveBeenCalled()
    expect(navigate).not.toHaveBeenCalled()
  })

  it.each([
    ['http', 'https://example.com/health?full=1'],
    ['tcp', 'db.example.com:5432'],
    ['tcp', '[::1]:5432'],
    ['ping', '10.0.0.1'],
    ['dns', 'example.com'],
  ] as const)('sends a valid %s target of %j to the server unchanged', async (kind, target) => {
    // A check stricter than the probe would refuse targets it can monitor, leaving the user no way to add them.
    await submit(kind, target)

    await waitFor(() => expect(client.command).toHaveBeenCalledTimes(1))
    expect(vi.mocked(client.command).mock.calls[0][1].target.value).toBe(target)
    expect(vi.mocked(client.command).mock.calls[0][1].kind.value).toBe(kind)
  })
})

describe('a create error the command runner did not record', () => {
  let unhandled: unknown[]
  let reporters: NodeJS.UnhandledRejectionListener[]
  const capture = (reason: unknown) => {
    unhandled.push(reason)
  }

  beforeEach(() => {
    navigate.mockClear()
    unhandled = []
    reporters = process.listeners('unhandledRejection')
    process.removeAllListeners('unhandledRejection')
    process.on('unhandledRejection', capture)
  })

  afterEach(() => {
    process.off('unhandledRejection', capture)
    for (const reporter of reporters) process.on('unhandledRejection', reporter)
  })

  it('is rethrown out of the submit instead of being swallowed', async () => {
    // The form only shows errors the runner recorded, so swallowing any other one would leave a failed create with no trace anywhere.
    const pending = () => new Promise<never>(() => undefined)
    const command = vi.fn(async () => [])
    // Only a hand-built row can carry an id that is not a uuid; the engine never returns one.
    const store = new Store({
      query: pending,
      command,
      unsubscribe: async () => undefined,
      subscribe: async (rql, _params, _shape, callbacks) => {
        if (rql !== regions.rql) return pending()
        callbacks.onInsert?.([{ '#rownum': 1, id: 'not-a-uuid', label: 'US East' }])
        return 'regions'
      },
    })
    renderWithProviders(<MonitorNewPage />, store)
    await fillAndSubmit('http', 'https://example.com')

    await waitFor(() => expect(unhandled).toEqual([expect.objectContaining({ message: 'Invalid UUID format: not-a-uuid' })]))
    expect(command).not.toHaveBeenCalled()
    expect(navigate).not.toHaveBeenCalled()
    expect(screen.queryByText(/Invalid UUID format/)).not.toBeInTheDocument()
  })
})
