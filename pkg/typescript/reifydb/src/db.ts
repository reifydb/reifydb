// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { SubscriptionConfig } from '@reifydb/client'
import type { BatchSubscribed, SubscriptionTick } from '../native'

export interface SubscriptionInput {
  query: string
  params?: any
  options?: SubscriptionConfig
}

export interface Db {
  adminRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  commandRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  queryRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  adminAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  commandAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  queryAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  authenticate(method: string, credentials: Record<string, string>): Promise<string>

  /**
   * `query` is the bare query and `options` carries hydration, throttle and linger, the same as a
   * client puts on a socket. Changes are not pushed: they come back from {@link Db.tick}.
   */
  subscribeRoot(query: string, params: any, options?: SubscriptionConfig): Promise<string>
  subscribeAs(identity: string, query: string, params: any, options?: SubscriptionConfig): Promise<string>
  batchSubscribeRoot(subscriptions: SubscriptionInput[]): Promise<BatchSubscribed>
  batchSubscribeAs(identity: string, subscriptions: SubscriptionInput[]): Promise<BatchSubscribed>
  unsubscribe(subscriptionId: string): void
  batchUnsubscribe(batchId: string): Promise<void>

  /** Advances the subscription pipeline by one pass and returns what it produced. */
  tick(): SubscriptionTick

  /** Resolves once every write that committed before the call has been staged and drained. */
  caughtUp(): Promise<SubscriptionTick>
}

export interface TestDb extends Db {}

export interface TestFactory {
  (): TestDb
}
