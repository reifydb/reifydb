// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  BinaryKind,
  CONTENT_TYPE_RBCF,
  buildSubscriptionRql,
  decodeBatchEnvelope,
  decodeEnvelope,
  dispatchChange,
  rbcf,
  reportSubscriptionError,
} from '@reifydb/client'
import type { BatchSubscription, BatchSubscriptionMember, SubscriptionTarget } from '@reifydb/client'
import type { StoreClient } from '@reifydb/store'
import type { SubscriptionTick } from '../native'
import type { Db } from './db'

export interface BridgeClient extends StoreClient {
  /**
   * Resolves once every change produced by a write that committed before the call has been
   * delivered to its subscription callbacks. Rejects if a subscription overran its queue.
   */
  caughtUp(): Promise<void>

  batchSubscribe(members: BatchSubscriptionMember[]): Promise<BatchSubscription>
  batchUnsubscribe(batchId: string): Promise<void>
}

export interface StoreClientOptions {
  // Runs every statement and every subscription as this identity instead of root. The engine leaves
  // $identity unset for root, so anything reading $identity needs a real user here.
  identity?: string
}

export function storeClient(db: Db, options: StoreClientOptions = {}): BridgeClient {
  const identity = options.identity
  const targets = new Map<string, SubscriptionTarget>()
  const batches = new Map<string, string[]>()

  const deliver = (subscriptionId: string, bytes: Uint8Array) => {
    const target = targets.get(subscriptionId)
    // A change for an unknown id is normal: unsubscribe drops the target while the pass that was
    // already staged is still in flight.
    if (!target) return
    let frames: any[]
    try {
      frames = rbcf.decode(bytes)
    } catch (error) {
      reportSubscriptionError(target, error)
      return
    }
    dispatchChange(target, CONTENT_TYPE_RBCF, { frames })
  }

  const dispatchEnvelope = (bytes: Uint8Array) => {
    if (bytes.length > 0 && bytes[0] === BinaryKind.BatchChange) {
      const batch = decodeBatchEnvelope(bytes)
      if (!batch) return
      for (const entry of batch.entries) deliver(entry.subscriptionId, entry.rbcf)
      return
    }
    const envelope = decodeEnvelope(bytes)
    if (!envelope || envelope.kind !== BinaryKind.Change) return
    deliver(envelope.id, envelope.rbcf)
  }

  // Synchronous so caughtUp resolves only after the callbacks have actually run.
  const dispatch = (tick: SubscriptionTick) => {
    for (const envelope of tick.envelopes) dispatchEnvelope(envelope)
    for (const subscriptionId of tick.closed) targets.delete(subscriptionId)
    for (const member of tick.batchMemberClosed) targets.delete(member.subscriptionId)
  }

  const reachCaughtUp = async () => {
    dispatch(await db.caughtUp())
  }

  // Nothing may drain between a subscribe returning its id and that id being registered, or the
  // staged hydration is read off the queue, matched to no target and dropped for good.
  let queue: Promise<unknown> = Promise.resolve()
  const alone = <T>(work: () => Promise<T>): Promise<T> => {
    const next = queue.then(work, work)
    queue = next.then(
      () => undefined,
      () => undefined,
    )
    return next
  }

  return {
    query: (rql, params, shapes) =>
      identity ? db.queryAs(identity, rql, params, shapes) : db.queryRoot(rql, params, shapes),
    command: (rql, params, shapes) =>
      identity ? db.commandAs(identity, rql, params, shapes) : db.commandRoot(rql, params, shapes),
    admin: (rql, params, shapes) =>
      identity ? db.adminAs(identity, rql, params, shapes) : db.adminRoot(rql, params, shapes),
    subscribe: (rql, params, shape, callbacks, config) =>
      alone(async () => {
        const statement = buildSubscriptionRql(rql, config)
        const subscriptionId = identity
          ? await db.subscribeAs(identity, statement, params)
          : await db.subscribeRoot(statement, params)
        targets.set(subscriptionId, { callbacks, shape })
        await reachCaughtUp()
        return subscriptionId
      }),
    unsubscribe: async (subscriptionId) => {
      targets.delete(subscriptionId)
      db.unsubscribe(subscriptionId)
    },
    batchSubscribe: (members) =>
      alone(async () => {
        if (members.length === 0) throw new Error('batchSubscribe requires at least one member')
        const queries = members.map((member) => buildSubscriptionRql(member.rql, member.config))
        const ack = identity
          ? await db.batchSubscribeAs(identity, queries)
          : await db.batchSubscribeRoot(queries)

        const subscriptionIds: string[] = new Array(members.length)
        for (const acked of ack.members) {
          const member = members[acked.index]
          if (!member) continue
          subscriptionIds[acked.index] = acked.subscriptionId
          targets.set(acked.subscriptionId, { callbacks: member.callbacks, shape: member.shape })
        }
        batches.set(ack.batchId, subscriptionIds.filter((id) => id !== undefined))

        await reachCaughtUp()
        return { batchId: ack.batchId, subscriptionIds }
      }),
    batchUnsubscribe: async (batchId) => {
      for (const subscriptionId of batches.get(batchId) ?? []) targets.delete(subscriptionId)
      batches.delete(batchId)
      await db.batchUnsubscribe(batchId)
    },
    caughtUp: () => alone(reachCaughtUp),
  }
}
