// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactElement } from 'react'
import { render } from '@testing-library/react'
import { Shape, Store, StoreProvider, type StoreClient } from '@reifydb/react'
import type { TestDb } from '@reifydb/reifydb'
import { vi } from 'vitest'

// Never settles, so a subscription the test did not seed stays loading instead of flipping state outside act().
function pendingClient(): StoreClient {
  const pending = () => new Promise<never>(() => undefined)
  return { query: pending, command: pending, subscribe: pending, unsubscribe: pending }
}

export function seededStore(): Store {
  return new Store(pendingClient())
}

// The engine leaves $identity unset for root and uptime::create_monitor reads $identity.id, so the bridge runs as a real user.
export async function bridgeStore(db: TestDb, user: string): Promise<{ store: Store; client: StoreClient }> {
  await db.adminRoot(`CREATE USER ${user}`, {}, [])
  const [[row]] = await db.queryRoot(
    'from system::identities filter { name == $name } map { id }',
    { name: user },
    [Shape.object({ id: Shape.identityid() })],
  )
  const identity = row.id
  const noSubscriptions = () => Promise.reject(new Error('the native bridge has no subscription support'))
  const client: StoreClient = {
    query: (rql, params, shapes) => db.queryAs(identity, rql, params, shapes),
    command: (rql, params, shapes) => db.commandAs(identity, rql, params, shapes),
    subscribe: noSubscriptions,
    unsubscribe: noSubscriptions,
  }
  vi.spyOn(client, 'command')
  return { store: new Store(client), client }
}

export function renderWithProviders(ui: ReactElement, store: Store) {
  return render(<StoreProvider store={store}>{ui}</StoreProvider>)
}
