// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactElement } from 'react'
import { render } from '@testing-library/react'
import {
  entryKey,
  Shape,
  Store,
  StoreProvider,
  type ReadSpec,
  type ShapeNode,
  type StoreClient,
  type StoreOptions,
} from '@reifydb/react'
import { storeClient, type BridgeClient, type TestDb } from '@reifydb/reifydb'
import { vi } from 'vitest'
import { STORE_OPTIONS } from '@/store/client'

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

// A batch refusal would fail every subscription the page opens, so batching is off and only this subscribe is refused.
export function refusingStore<S extends ShapeNode, P extends object | null>(
  client: StoreClient,
  spec: ReadSpec<S, P>,
  params: NoInfer<P>,
  error: Error,
): Store {
  const refused = entryKey(spec.rql, params, spec.shape)
  return new Store(
    {
      ...client,
      batchSubscribe: undefined,
      subscribe: (rql, subscriptionParams, shape, callbacks, config) =>
        entryKey(rql, subscriptionParams, shape) === refused
          ? Promise.reject(error)
          : client.subscribe(rql, subscriptionParams, shape, callbacks, config),
    },
    { ...STORE_OPTIONS, batch: false },
  )
}

export function renderWithProviders(ui: ReactElement, store: Store) {
  return render(<StoreProvider store={store}>{ui}</StoreProvider>)
}
