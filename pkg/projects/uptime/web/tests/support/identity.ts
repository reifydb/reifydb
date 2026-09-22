// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Shape, Store, rql, type StoreClient } from '@reifydb/react'
import { storeClient, type TestDb } from '@reifydb/reifydb'
import { STORE_OPTIONS } from '@/store/client'
import { queryRoot } from './db'

export const ME_QUERY_CONTRACT = 'map { id: $identity.id, name: $identity.name, kind: $identity.kind }'

const identityByName = rql(Shape.object({ id: Shape.identityid() }))<{
  name: string
}>`from system::identities filter { name == $name } map { id }`

export async function identityNamed(db: TestDb, name: string): Promise<string> {
  const [row] = await queryRoot(db, identityByName, { name })
  return row.id
}

export async function createUserWithEmail(db: TestDb, email: string): Promise<string> {
  await db.adminRoot(`CREATE USER \`${email}\` { email: $email }`, { email }, [])
  return identityNamed(db, email)
}

export function storeAs(db: TestDb, identity: string): Store {
  return new Store(storeClient(db, { identity }), STORE_OPTIONS)
}

export function guestStore(id: string): Store {
  // The bridge cannot mint a guest identity, so this answers exactly the proven me query and refuses anything else.
  const pending = () => new Promise<never>(() => undefined)
  const client: StoreClient = {
    query: (async (rql: string) => {
      if (rql !== ME_QUERY_CONTRACT) throw new Error(`the guest client only answers the me query, got: ${rql}`)
      return [[{ id, name: `guest:${id}`, kind: 'guest' }]]
    }) as unknown as StoreClient['query'],
    command: pending,
    admin: pending,
    subscribe: pending,
    unsubscribe: async () => undefined,
  }
  return new Store(client)
}
