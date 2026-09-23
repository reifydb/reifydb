// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {WsClient} from '@reifydb/client';
import type {InferShape, ShapeNode} from '@reifydb/core';
import {rql} from '../../src';
import type {ReadSpec, Store, WriteSpec} from '../../src';
import {waitFor} from './setup';

let counter = 0;

// Each table is unique so a subscription in one test never sees a row written by another.
export function tableName(prefix: string): string {
    counter += 1;
    return `${prefix}_${counter}_${Math.random().toString(36).slice(2, 8)}`;
}

function template(text: string): TemplateStringsArray {
    // The tag refuses substitutions, so runtime table names must reach it as one pre-joined string.
    return Object.assign([text], {raw: [text]});
}

export function readSpec<const S extends ShapeNode | readonly ShapeNode[]>(shape: S, text: string): ReadSpec<S, any> {
    return rql(shape)<any>(template(text));
}

export function writeSpec<const S extends readonly ShapeNode[]>(shapes: S, text: string): WriteSpec<S, any> {
    return rql.write(shapes)<any>(template(text));
}

export async function createTable(client: WsClient, table: string, columns: string): Promise<void> {
    await client.admin(`create table ${table} { ${columns} }`, null, []);
}

export function rowsOf<S extends ShapeNode>(store: Store, rql: string, shape: S): InferShape<S>[] {
    return store.getEntry(readSpec(shape, rql), null).data as InferShape<S>[];
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
    return waitFor(store, () => store.getEntry(readSpec(shape, rql), null).status === 'ready');
}
