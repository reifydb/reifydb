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

function fakeDb(calls: Call[]): Db {
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

  it('subscribe rejects with an error stating that the bridge has no subscription support', async () => {
    const calls: Call[] = []
    await expect(storeClient(fakeDb(calls)).subscribe('from t', null, shape, {})).rejects.toThrow(/bridge has no subscription support/)
    expect(calls).toEqual([])
  })

  it('unsubscribe rejects with an error stating that the bridge has no subscription support', async () => {
    const calls: Call[] = []
    await expect(storeClient(fakeDb(calls)).unsubscribe('sub-1')).rejects.toThrow(/bridge has no subscription support/)
    expect(calls).toEqual([])
  })
})
