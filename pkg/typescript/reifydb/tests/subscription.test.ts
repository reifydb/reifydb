// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// End-to-end coverage of subscriptions over the native bridge.
//
// Every hop is real: the statement is built by the same client helper a WebSocket subscription uses,
// the change crosses the flow subsystem, the sink encodes it into the same rbcf envelope the socket
// transport puts on the wire, and the client's own decoder turns it back into callback rows. A unit
// test with a fake Db would exercise none of that, which is exactly where a bridge breaks.

import { beforeEach, describe, expect, it } from 'vitest'
import { Shape } from '@reifydb/core'
import type { SubscriptionRow } from '@reifydb/client'
import { Reifydb } from '../src/reifydb'
import { storeClient, type BridgeClient } from '../src/store-client'
import type { Db } from '../src/db'

const rowShape = Shape.object({ id: Shape.int4(), val: Shape.int4() })

interface Seen {
  inserted: SubscriptionRow<any>[]
  updated: SubscriptionRow<any>[]
  removed: SubscriptionRow<any>[]
  errors: Error[]
}

function collector(): Seen {
  return { inserted: [], updated: [], removed: [], errors: [] }
}

function callbacks(seen: Seen) {
  return {
    onInsert: (rows: SubscriptionRow<any>[]) => seen.inserted.push(...rows),
    onUpdate: (rows: SubscriptionRow<any>[]) => seen.updated.push(...rows),
    onRemove: (rows: SubscriptionRow<any>[]) => seen.removed.push(...rows),
    onError: (error: Error) => seen.errors.push(error),
  }
}

function build(): Db {
  return Reifydb.memory()
    .withMigrations({
      name: 'init',
      statements: [
        'create namespace app',
        'create table app::t { id: int4, val: int4 }',
        'create deferred view app::v { id: int4, val: int4 } as { from app::t map { id, val } }',
        // Root subscribes without one, but any other identity is denied by SESSION_001 until a
        // session policy grants it, which is what the identity test below depends on.
        'create session policy allow_subscribe { subscription: { filter { true } } }',
        'create table policy app_t_all on app::t { from: { filter { true } }, insert: { filter { true } } }',
        'create view policy app_v_all on app::v { from: { filter { true } } }',
      ],
    })
    .build()
}

// Created per test, not in the shared migration the single-subscription tests use.
async function secondView(db: Db): Promise<void> {
  await db.adminRoot('create table app::u { id: int4, val: int4 }', null, [])
  await db.adminRoot(
    'create deferred view app::w { id: int4, val: int4 } as { from app::u map { id, val } }',
    null,
    [],
  )
}

async function viewRowCount(client: BridgeClient): Promise<number> {
  const [rows] = await client.query('from app::v map { id, val }', null, [rowShape])
  return rows.length
}

describe('subscriptions over the native bridge', () => {
  let db: Db
  let client: BridgeClient

  beforeEach(() => {
    db = build()
    client = storeClient(db)
  })

  it('delivers a write that had to cross the flow subsystem', async () => {
    // A deferred view is what uptime subscribes to. The write has to reach the cdc producer, be
    // materialized by the flow actors and only then be staged, so this is the hop count that
    // actually matters; a table subscription would settle in one pass and prove much less.
    const seen = collector()
    await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen))
    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.caughtUp()

    expect(seen.inserted).toEqual([expect.objectContaining({ id: 1, val: 10 })])
    expect(seen.errors).toEqual([])
  })

  it('delivers rows that predate the subscription', async () => {
    // Uptime subscribes to views that already hold data. Without hydration the page would render
    // empty until the next write happened to arrive, which is not a state the app ever recovers
    // from on its own.
    await client.command('insert app::t [{ id: 1, val: 10 }, { id: 2, val: 20 }]', null, [])

    const seen = collector()
    // No barrier here on purpose: hydration is part of what subscribe promises.
    await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen))

    expect(seen.inserted.map((row) => row.id).sort()).toEqual([1, 2])
  })

  it('starts empty when hydration is switched off', async () => {
    // The counterpart to the test above: it is what shows the rows there came from hydration and
    // not from forward changes that happened to arrive late.
    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.caughtUp()
    expect(await viewRowCount(client)).toBe(1)

    const seen = collector()
    await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen), {
      hydration: { enabled: false },
    })

    await client.command('insert app::t [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    expect(seen.inserted.map((row) => row.id)).toEqual([2])
    expect(seen.errors).toEqual([])
  })

  it('reports an update and a remove as their own operations', async () => {
    // The diff kind travels in the encoded frame, so a bridge that framed changes correctly but
    // dropped the op would deliver every change as an insert and the store would never remove a row.
    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])

    const seen = collector()
    await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen))

    await client.command('update app::t { val: 99 } filter { id == 1 }', null, [])
    await client.caughtUp()
    expect(seen.updated).toEqual([expect.objectContaining({ id: 1, val: 99 })])

    await client.command('delete app::t filter { id == 1 }', null, [])
    await client.caughtUp()
    expect(seen.removed).toEqual([expect.objectContaining({ id: 1 })])
  })

  it('stops delivering once unsubscribed', async () => {
    // A subscription that kept delivering after unsubscribe would leak into a torn-down component,
    // and the only place that shows up is a test that keeps writing afterwards.
    const seen = collector()
    const subscriptionId = await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen))
    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.caughtUp()

    await client.unsubscribe(subscriptionId)
    await client.command('insert app::t [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    expect(seen.inserted.map((row) => row.id)).toEqual([1])
  })

  it('keeps two independent subscriptions from crossing', async () => {
    // Each subscription gets its own envelope keyed by its own id, and one client holds them all in
    // one target map. A mixed-up id would hand a subscription the other one's rows, which a single
    // subscription could never catch.
    await secondView(db)

    const first = collector()
    const second = collector()
    await client.subscribe('from app::v map { id, val }', null, rowShape, callbacks(first))
    await client.subscribe('from app::w map { id, val }', null, rowShape, callbacks(second))

    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.command('insert app::u [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    expect(first.inserted).toEqual([expect.objectContaining({ id: 1, val: 10 })])
    expect(second.inserted).toEqual([expect.objectContaining({ id: 2, val: 20 })])
  })

  it('routes each entry of a batch envelope to the member that owns it', async () => {
    // A batch change is one envelope carrying every member's rows keyed by subscription id, which
    // is a different frame from the single-subscription one. Two members over two views is the
    // smallest case that can tell a correct decode from one that hands a member the wrong entry.
    await secondView(db)

    const first = collector()
    const second = collector()
    const batch = await client.batchSubscribe([
      { rql: 'from app::v map { id, val }', shape: rowShape, callbacks: callbacks(first) },
      { rql: 'from app::w map { id, val }', shape: rowShape, callbacks: callbacks(second) },
    ])

    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.command('insert app::u [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    expect(first.inserted).toEqual([expect.objectContaining({ id: 1, val: 10 })])
    expect(second.inserted).toEqual([expect.objectContaining({ id: 2, val: 20 })])
    expect(first.errors).toEqual([])
    expect(second.errors).toEqual([])
    expect(batch.subscriptionIds).toHaveLength(2)
  })

  it('returns the member ids in the order the members were given', async () => {
    // The ack reports each member by index, not by position in its own list. If that index were
    // ignored the ids would still all be present and every callback would still fire, so only
    // unsubscribing one specific member can show the mapping is right.
    await secondView(db)

    const first = collector()
    const second = collector()
    const batch = await client.batchSubscribe([
      { rql: 'from app::v map { id, val }', shape: rowShape, callbacks: callbacks(first) },
      { rql: 'from app::w map { id, val }', shape: rowShape, callbacks: callbacks(second) },
    ])

    await client.unsubscribe(batch.subscriptionIds[0])

    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.command('insert app::u [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    expect(first.inserted).toEqual([])
    expect(second.inserted).toEqual([expect.objectContaining({ id: 2, val: 20 })])
  })

  it('hydrates every member of a batch from rows that predate it', async () => {
    // Hydration of a batch member takes its own path: the rows are wrapped into a batch envelope
    // rather than sent as a plain change, so a batch can hydrate wrongly while a lone subscription
    // hydrates fine.
    await secondView(db)
    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.command('insert app::u [{ id: 2, val: 20 }]', null, [])
    await client.caughtUp()

    const first = collector()
    const second = collector()
    await client.batchSubscribe([
      { rql: 'from app::v map { id, val }', shape: rowShape, callbacks: callbacks(first) },
      { rql: 'from app::w map { id, val }', shape: rowShape, callbacks: callbacks(second) },
    ])

    expect(first.inserted).toEqual([expect.objectContaining({ id: 1, val: 10 })])
    expect(second.inserted).toEqual([expect.objectContaining({ id: 2, val: 20 })])
  })

  it('stops delivering to every member once the batch is unsubscribed', async () => {
    // Unsubscribing the batch has to reach all of its members. Dropping the batch while leaving a
    // member registered would keep delivering rows to a caller that asked to be done.
    await secondView(db)

    const first = collector()
    const second = collector()
    const batch = await client.batchSubscribe([
      { rql: 'from app::v map { id, val }', shape: rowShape, callbacks: callbacks(first) },
      { rql: 'from app::w map { id, val }', shape: rowShape, callbacks: callbacks(second) },
    ])

    await client.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await client.caughtUp()
    await client.batchUnsubscribe(batch.batchId)

    await client.command('insert app::t [{ id: 3, val: 30 }]', null, [])
    await client.command('insert app::u [{ id: 4, val: 40 }]', null, [])
    await client.caughtUp()

    expect(first.inserted.map((row) => row.id)).toEqual([1])
    expect(second.inserted).toEqual([])
  })

  it('refuses a batch with no members', async () => {
    await expect(client.batchSubscribe([])).rejects.toThrow(/at least one member/)
  })

  it('runs a subscription as the configured identity', async () => {
    // The engine leaves $identity unset for root, so anything reading it needs a real user. A client
    // built with an identity has to carry it into subscribe as well as into query and command,
    // otherwise a subscription silently runs with more access than the session it belongs to.
    await db.adminRoot('create user alice', null, [])
    const [[user]] = await db.queryRoot(
      'from system::identities filter { name == $name } map { id }',
      { name: 'alice' },
      [Shape.object({ id: Shape.identityid() })],
    )
    const asAlice = storeClient(db, { identity: user.id })

    const seen = collector()
    await asAlice.subscribe('from app::v map { id, val }', null, rowShape, callbacks(seen))
    await asAlice.command('insert app::t [{ id: 1, val: 10 }]', null, [])
    await asAlice.caughtUp()

    expect(seen.inserted).toEqual([expect.objectContaining({ id: 1, val: 10 })])
  })
})
