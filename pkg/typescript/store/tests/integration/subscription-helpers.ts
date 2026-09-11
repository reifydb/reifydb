// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {WsClient} from '@reifydb/client';
import type {InferShape, ShapeNode} from '@reifydb/core';
import type {Store} from '../../src';
import {waitFor} from './setup';

let counter = 0;

// Each table is unique so a subscription in one test never sees a row written by another.
export function tableName(prefix: string): string {
    counter += 1;
    return `${prefix}_${counter}_${Math.random().toString(36).slice(2, 8)}`;
}

export async function createTable(client: WsClient, table: string, columns: string): Promise<void> {
    await client.admin(`create table ${table} { ${columns} }`, null, []);
}

export function rowsOf<S extends ShapeNode>(store: Store, rql: string, shape: S): InferShape<S>[] {
    return store.getEntry(rql, null, shape).data as InferShape<S>[];
}

export function waitForRows<S extends ShapeNode>(
    store: Store,
    rql: string,
    shape: S,
    count: number
): Promise<void> {
    return waitFor(store, () => rowsOf(store, rql, shape).length === count);
}

export function waitForReady(store: Store, rql: string, shape: ShapeNode): Promise<void> {
    return waitFor(store, () => store.getEntry(rql, null, shape).status === 'ready');
}
