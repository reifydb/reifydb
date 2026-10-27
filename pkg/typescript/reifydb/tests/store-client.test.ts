// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from 'vitest'
import { Shape } from '@reifydb/core'
import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { Db } from '../src/db'
import { storeClient } from '../src/store-client'

interface Call {
  method: string
  rql: string
  params: any
  shapes: readonly ShapeNode[]
}

function fakeDb(calls: Call[], overrides: Partial<Db> = {}): Db {
  const record = (method: string) =>
    <const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> => {
      calls.push({ method, rql, params, shapes })
      return Promise.resolve([] as FrameResults<S>)
    }
  const unused = () => Promise.reject(new Error('not routed here'))
  return {
    queryRoot: record('queryRoot'),
    commandRoot: record('commandRoot'),
    adminRoot: record('adminRoot'),
    queryAs: unused,
    commandAs: unused,
    adminAs: unused,
    authenticate: unused,
    subscribeAs: unused,
    unsubscribe: () => Promise.resolve(),
    rootIdentity: () => 'root',
    tick: () => ({ envelopes: [], closed: [] }),
    caughtUp: () => Promise.resolve({ envelopes: [], closed: [] }),
    ...overrides,
  }
}

const shape = Shape.object({ id: Shape.int4() })

describe('storeClient', () => {
  it('routes query to queryRoot with rql, params and shapes unchanged', async () => {
    const calls: Call[] = []
    await storeClient(fakeDb(calls)).query('from t', { a: 1 }, [shape])
    expect(calls).toEqual([{ method: 'queryRoot', rql: 'from t', params: { a: 1 }, shapes: [shape] }])
  })

  it('routes command to commandRoot with rql, params and shapes unchanged', async () => {
    const calls: Call[] = []
    await storeClient(fakeDb(calls)).command('insert t [{ id: 1 }]', null, [])
    expect(calls).toEqual([{ method: 'commandRoot', rql: 'insert t [{ id: 1 }]', params: null, shapes: [] }])
  })

  it('routes admin to adminRoot with rql, params and shapes unchanged', async () => {
    const calls: Call[] = []
    await storeClient(fakeDb(calls)).admin('create table t { id: int4 }', null, [shape])
    expect(calls).toEqual([{ method: 'adminRoot', rql: 'create table t { id: int4 }', params: null, shapes: [shape] }])
  })

  it('tells only the closed subscription that the server closed it, exactly once', async () => {
    // Without calling onClosed on tick.closed the store never learns the server dropped the subscription.
    let closedTicks = 0
    const db = fakeDb([], {
      subscribeAs: () => Promise.resolve(['7', '8']),
      caughtUp: () => Promise.resolve({ envelopes: [], closed: closedTicks++ === 0 ? ['7'] : [] }),
    })
    const client = storeClient(db)
    const closed: string[] = []
    await client.subscribe([
      { rql: 'from t', shape, callbacks: { onClosed: () => closed.push('7') } },
      { rql: 'from u', shape, callbacks: { onClosed: () => closed.push('8') } },
    ])
    await client.caughtUp()
    expect(closed).toEqual(['7'])
  })

  it('releases every id of one unsubscribe in a single call to the database', async () => {
    // A per-id loop would cost one native round trip per subscription instead of one per release.
    const unsubscribed: string[][] = []
    const db = fakeDb([], {
      unsubscribe: (subscriptionIds) => {
        unsubscribed.push(subscriptionIds)
        return Promise.resolve()
      },
    })
    await storeClient(db).unsubscribe(['7', '8'])
    expect(unsubscribed).toEqual([['7', '8']])
  })
})
