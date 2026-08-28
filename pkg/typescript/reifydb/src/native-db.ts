// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { encodeParams, columnsToRows, transformFrames } from '@reifydb/core'
import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { Frame, ReifydbNode } from '../native'
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

  private async execute<const S extends readonly ShapeNode[]>(pending: Promise<Frame[]>, shapes: S): Promise<FrameResults<S>> {
    const frames = await pending
    const rows = frames.map((frame) => columnsToRows(frame.columns))
    return transformFrames(rows, shapes)
  }
}

function toWireParams(params: any) {
  return params !== undefined && params !== null ? encodeParams(params) : undefined
}
