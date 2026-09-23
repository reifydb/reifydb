// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FrameResults, InferShape, ReadSpec, ShapeNode, WriteSpec } from '@reifydb/react'
import type { TestDb } from '@reifydb/reifydb'

export async function queryRoot<S extends ShapeNode, P extends object | null>(
  db: TestDb,
  spec: ReadSpec<S, P>,
  params: NoInfer<P>,
): Promise<InferShape<S>[]> {
  const [rows] = await db.queryRoot(spec.rql, params, [spec.shape])
  return rows as InferShape<S>[]
}

export function commandRoot<const S extends readonly ShapeNode[], P extends object | null>(
  db: TestDb,
  spec: WriteSpec<S, P>,
  params: NoInfer<P>,
): Promise<FrameResults<S>> {
  return db.commandRoot(spec.rql, params, spec.shape)
}

export function commandAs<const S extends readonly ShapeNode[], P extends object | null>(
  db: TestDb,
  identity: string,
  spec: WriteSpec<S, P>,
  params: NoInfer<P>,
): Promise<FrameResults<S>> {
  return db.commandAs(identity, spec.rql, params, spec.shape)
}

export function adminRoot<const S extends readonly ShapeNode[], P extends object | null>(
  db: TestDb,
  spec: WriteSpec<S, P>,
  params: NoInfer<P>,
): Promise<FrameResults<S>> {
  return db.adminRoot(spec.rql, params, spec.shape)
}
