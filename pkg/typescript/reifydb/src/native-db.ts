// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { encodeParams, columnsToRows, transformFrames, checkFrames, framesFromWire } from '@reifydb/core'
import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { BatchSubscribed, Frame, ReifydbNode, SubscriptionTick } from '../native'
import type { Db } from './db'

export class NativeDb implements Db {
  constructor(private readonly node: ReifydbNode) {}

  adminRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.adminRoot(rql, toWireParams(params)), shapes)
  }

  commandRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.commandRoot(rql, toWireParams(params)), shapes)
  }

  queryRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.queryRoot(rql, toWireParams(params)), shapes)
  }

  adminAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.adminAs(identity, rql, toWireParams(params)), shapes)
  }

  commandAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.commandAs(identity, rql, toWireParams(params)), shapes)
  }

  queryAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.queryAs(identity, rql, toWireParams(params)), shapes)
  }

  authenticate(method: string, credentials: Record<string, string>): Promise<string> {
    return this.node.authenticate(method, credentials)
  }

  subscribeRoot(rql: string, params: any): Promise<string> {
    return this.node.subscribeRoot(rql, toWireParams(params))
  }

  subscribeAs(identity: string, rql: string, params: any): Promise<string> {
    return this.node.subscribeAs(identity, rql, toWireParams(params))
  }

  batchSubscribeRoot(queries: string[]): Promise<BatchSubscribed> {
    return this.node.batchSubscribeRoot(queries)
  }

  batchSubscribeAs(identity: string, queries: string[]): Promise<BatchSubscribed> {
    return this.node.batchSubscribeAs(identity, queries)
  }

  unsubscribe(subscriptionId: string): void {
    this.node.unsubscribe(subscriptionId)
  }

  batchUnsubscribe(batchId: string): Promise<void> {
    return this.node.batchUnsubscribe(batchId)
  }

  tick(): SubscriptionTick {
    return this.node.tick()
  }

  async caughtUp(): Promise<SubscriptionTick> {
    return this.node.caughtUp()
  }

  private async execute<const S extends readonly ShapeNode[]>(pending: Promise<Frame[]>, shapes: S): Promise<FrameResults<S>> {
    // The native binding renders frames the same way the server does, so the column types arrive in the
    // wire's rendering and have to be read into the client's own before anything decodes them.
    const frames = framesFromWire(await pending)
    checkFrames(frames, shapes)
    const rows = frames.map((frame) => columnsToRows(frame.columns))
    return transformFrames(rows, shapes)
  }
}

function toWireParams(params: any) {
  return params !== undefined && params !== null ? encodeParams(params) : undefined
}
