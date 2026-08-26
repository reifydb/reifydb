// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { encode_params, columns_to_rows, transform_frames } from '@reifydb/core'
import type { FrameResults, ShapeNode } from '@reifydb/core'
import type { Frame, ReifydbNode } from '../native'
import type { Db } from './db'

export class NativeDb implements Db {
  constructor(private readonly node: ReifydbNode) {}

  admin_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.admin_root(rql, to_wire_params(params)), shapes)
  }

  command_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.command_root(rql, to_wire_params(params)), shapes)
  }

  query_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.query_root(rql, to_wire_params(params)), shapes)
  }

  admin_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.admin_as(identity, rql, to_wire_params(params)), shapes)
  }

  command_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.command_as(identity, rql, to_wire_params(params)), shapes)
  }

  query_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>> {
    return this.execute(this.node.query_as(identity, rql, to_wire_params(params)), shapes)
  }

  authenticate(method: string, credentials: Record<string, string>): Promise<string> {
    return this.node.authenticate(method, credentials)
  }

  private async execute<const S extends readonly ShapeNode[]>(pending: Promise<Frame[]>, shapes: S): Promise<FrameResults<S>> {
    const frames = await pending
    const rows = frames.map((frame) => columns_to_rows(frame.columns))
    return transform_frames(rows, shapes)
  }
}

function to_wire_params(params: any) {
  return params !== undefined && params !== null ? encode_params(params) : undefined
}
