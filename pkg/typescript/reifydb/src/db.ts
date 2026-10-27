// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { SubscriptionConfig } from '@reifydb/client'
import type { SubscriptionTick } from '../native'

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
  subscribeAs(identity: string, subscriptions: SubscriptionInput[]): Promise<string[]>
  unsubscribe(subscriptionIds: string[]): Promise<void>
  rootIdentity(): string

  /** Advances the subscription pipeline by one pass and returns what it produced. */
  tick(): SubscriptionTick

  /** Resolves once every write that committed before the call has been staged and drained. */
  caughtUp(): Promise<SubscriptionTick>
}

export interface TestDb extends Db {}

export interface TestFactory {
  (): TestDb
}
