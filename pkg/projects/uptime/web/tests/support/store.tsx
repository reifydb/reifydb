// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactElement } from 'react'
import { act, render } from '@testing-library/react'
import {
  entryKey,
  rql,
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
import { queryRoot } from './db'

const identityByName = rql(Shape.object({ id: Shape.identityid() }))<{
  name: string
}>`from system::identities filter { name == $name } map { id }`

// The engine leaves $identity unset for root and uptime::create_monitor reads $identity.id, so the bridge runs as a real user.
export async function bridgeStore(
  db: TestDb,
  user: string,
  overrides: StoreOptions = {},
): Promise<{ store: Store; client: BridgeClient; identity: string }> {
  await db.adminRoot(`CREATE USER ${user}`, {}, [])
  const [row] = await queryRoot(db, identityByName, { name: user })
  const client = storeClient(db, { identity: row.id })
  vi.spyOn(client, 'command')
  vi.spyOn(client, 'subscribe')
  return { store: new Store(client, { ...STORE_OPTIONS, ...overrides }), client, identity: row.id }
}

export function refusingStore<P extends object | null>(
  client: StoreClient,
  spec: ReadSpec<ShapeNode, P>,
  params: NoInfer<P>,
  error: Error,
): { store: Store; refused: Promise<void> } {
  // Rejecting the whole call would fail every hook of the tick, and an onError before the ack is overwritten by 'ready'.
  const refused = entryKey(spec.rql, params, spec.shape)
  const placeholder = 'refused-subscription'
  let deliver!: () => void
  const delivered = new Promise<void>((resolve) => {
    deliver = resolve
  })
  const store = new Store(
    {
      ...client,
      subscribe: async (subscriptions) => {
        const index = subscriptions.findIndex(
          (subscription) =>
            subscription.shape !== undefined &&
            entryKey(subscription.rql, subscription.params, subscription.shape) === refused,
        )
        if (index === -1) return client.subscribe(subscriptions)
        const others = subscriptions.filter((_, position) => position !== index)
        const subscriptionIds = others.length > 0 ? await client.subscribe(others) : []
        setTimeout(() => {
          subscriptions[index].callbacks.onError?.(error)
          deliver()
        }, 0)
        return [...subscriptionIds.slice(0, index), placeholder, ...subscriptionIds.slice(index)]
      },
      unsubscribe: (subscriptionIds) => {
        const held = subscriptionIds.filter((subscriptionId) => subscriptionId !== placeholder)
        return held.length > 0 ? client.unsubscribe(held) : Promise.resolve()
      },
    },
    STORE_OPTIONS,
  )
  return { store, refused: delivered }
}

export async function caughtUp(store: Store, client: BridgeClient) {
  await act(async () => {
    await store.flushed()
    await client.caughtUp()
  })
}

export function renderWithProviders(ui: ReactElement, store: Store) {
  return render(<StoreProvider store={store}>{ui}</StoreProvider>)
}
