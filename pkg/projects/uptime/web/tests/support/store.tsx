// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactElement } from 'react'
import { render } from '@testing-library/react'
import { Shape, Store, StoreProvider, type StoreClient, type StoreOptions } from '@reifydb/react'
import { storeClient, type BridgeClient, type TestDb } from '@reifydb/reifydb'
import { vi } from 'vitest'
import { STORE_OPTIONS } from '@/store/client'

// Never settles, so a subscription the test did not seed stays loading instead of flipping state outside act().
function pendingClient(): StoreClient {
  const pending = () => new Promise<never>(() => undefined)
  return { query: pending, command: pending, subscribe: pending, unsubscribe: pending }
}

export function seededStore(): Store {
  return new Store(pendingClient())
}

// The engine leaves $identity unset for root and uptime::create_monitor reads $identity.id, so the bridge runs as a real user.
export async function bridgeStore(
  db: TestDb,
  user: string,
  overrides: StoreOptions = {},
): Promise<{ store: Store; client: BridgeClient }> {
  await db.adminRoot(`CREATE USER ${user}`, {}, [])
  const [[row]] = await db.queryRoot(
    'from system::identities filter { name == $name } map { id }',
    { name: user },
    [Shape.object({ id: Shape.identityid() })],
  )
  const client = storeClient(db, { identity: row.id })
  vi.spyOn(client, 'command')
  vi.spyOn(client, 'subscribe')
  vi.spyOn(client, 'batchSubscribe')
  return { store: new Store(client, { ...STORE_OPTIONS, ...overrides }), client }
}

export function renderWithProviders(ui: ReactElement, store: Store) {
  return render(<StoreProvider store={store}>{ui}</StoreProvider>)
}
