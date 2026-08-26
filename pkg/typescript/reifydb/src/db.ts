// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FrameResults, ShapeNode } from '@reifydb/core'

export interface Db {
  admin_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  command_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  query_root<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  admin_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  command_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  query_as<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  authenticate(method: string, credentials: Record<string, string>): Promise<string>
}

export interface TestDb extends Db {}

export interface TestFactory {
  (seed: number): TestDb
}
