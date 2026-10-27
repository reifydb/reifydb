// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  BinaryKind,
  CONTENT_TYPE_RBCF,
  decodeChangeListEnvelope,
  dispatchChange,
  rbcf,
  reportSubscriptionError,
} from '@reifydb/client'
import type { SubscriptionTarget } from '@reifydb/client'
import type { StoreClient } from '@reifydb/store'
import type { SubscriptionTick } from '../native'
import type { Db } from './db'

export interface BridgeClient extends StoreClient {
  /**
   * Resolves once every change produced by a write that committed before the call has been
   * delivered to its subscription callbacks. Rejects if a subscription overran its queue.
   */
  caughtUp(): Promise<void>
}

export interface StoreClientOptions {
  // Runs every statement and every subscription as this identity instead of root. The engine leaves
  // $identity unset for root, so anything reading $identity needs a real user here.
  identity?: string
}

export function storeClient(db: Db, options: StoreClientOptions = {}): BridgeClient {
  const identity = options.identity
  const targets = new Map<string, SubscriptionTarget>()

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
    if (bytes.length > 0 && bytes[0] === BinaryKind.Change) {
      for (const entry of decodeChangeListEnvelope(bytes).entries) deliver(entry.subscriptionId, entry.rbcf)
      return
    }
  }

  // Synchronous so caughtUp resolves only after the callbacks have actually run.
  const dispatch = (tick: SubscriptionTick) => {
    for (const envelope of tick.envelopes) dispatchEnvelope(envelope)
    for (const subscriptionId of tick.closed) {
      const target = targets.get(subscriptionId)
      targets.delete(subscriptionId)
      target?.callbacks.onClosed?.()
    }
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
    subscribe: (subscriptions) =>
      alone(async () => {
        if (subscriptions.length === 0) throw new Error('subscribe requires at least one subscription')
        const subscriptionIds = await db.subscribeAs(
          identity ?? db.rootIdentity(),
          subscriptions.map((subscription) => ({
            query: subscription.rql,
            params: subscription.params,
            options: subscription.config,
          })),
        )
        subscriptions.forEach((subscription, index) => {
          const subscriptionId = subscriptionIds[index]
          if (subscriptionId !== undefined) {
            targets.set(subscriptionId, { callbacks: subscription.callbacks, shape: subscription.shape })
          }
        })
        await reachCaughtUp()
        return subscriptionIds
      }),
    unsubscribe: async (subscriptionIds) => {
      for (const subscriptionId of subscriptionIds) targets.delete(subscriptionId)
      await db.unsubscribe(subscriptionIds)
    },
    caughtUp: () => alone(reachCaughtUp),
  }
}
