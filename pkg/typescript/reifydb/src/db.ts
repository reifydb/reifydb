// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FrameResults, ShapeNode } from '@reifydb/core'

export interface Db {
  adminRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  commandRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  queryRoot<const S extends readonly ShapeNode[]>(rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  adminAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  commandAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  queryAs<const S extends readonly ShapeNode[]>(identity: string, rql: string, params: any, shapes: S): Promise<FrameResults<S>>
  authenticate(method: string, credentials: Record<string, string>): Promise<string>
}

export interface TestDb extends Db {}

export interface TestFactory {
  (seed: number): TestDb
}
