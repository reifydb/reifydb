// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '../../src';
import {connect, namespace, waitFor} from './setup';
import {createTable, rowsOf, tableName, waitForReady, waitForRows} from './subscription-helpers';

const ns = namespace('sub_opt');
const optional = Shape.object({id: Shape.int4(), value: Shape.option(Shape.int4())});

describe.each(['frames', 'rbcf'] as const)('subscription option shapes (%s)', format => {
    let client: WsClient;
    let store: Store;

    beforeAll(async () => {
        client = await connect(format);
        await client.admin(`create namespace ${ns}`, null, []).catch(() => undefined);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    async function optionalTable(prefix: string): Promise<string> {
        const name = `${ns}::${tableName(`${prefix}_${format}`)}`;
        await createTable(client, name, 'id: int4, value: Option(int4)');
        return name;
    }

    it('a present optional value arrives as Some carrying the value', async () => {
        const name = await optionalTable('some');
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, optional);
        await waitForReady(store, rql, optional);
        await client.command(`insert ${name} [{ id: 1, value: 42 }]`, null, []);
        await waitForRows(store, rql, optional, 1);
        const row = rowsOf(store, rql, optional)[0];
        expect(row.value.isSome()).toBe(true);
        expect(row.value.unwrap()).toBe(42);
        release();
    });

    it('an absent optional value arrives as None, not as a zero or an empty string', async () => {
        const name = await optionalTable('none');
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, optional);
        await waitForReady(store, rql, optional);
        await client.command(`insert ${name} [{ id: 1, value: none }]`, null, []);
        await waitForRows(store, rql, optional, 1);
        const row = rowsOf(store, rql, optional)[0];
        expect(row.value.isNone()).toBe(true);
        expect(() => row.value.unwrap()).toThrow();
        release();
    });

    it('Some and None in the same batch stay distinguishable row by row', async () => {
        const name = await optionalTable('mixed');
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, optional);
        await waitForReady(store, rql, optional);
        await client.command(
            `insert ${name} [{ id: 1, value: 1 }, { id: 2, value: none }, { id: 3, value: 3 }]`,
            null,
            []
        );
        await waitForRows(store, rql, optional, 3);
        const byId = new Map(rowsOf(store, rql, optional).map(row => [row.id, row.value]));
        expect(byId.get(1)!.unwrap()).toBe(1);
        expect(byId.get(2)!.isNone()).toBe(true);
        expect(byId.get(3)!.unwrap()).toBe(3);
        release();
    });

    it('an UPDATE that clears the column flips Some to None on the diff path', async () => {
        const name = await optionalTable('clear');
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, optional);
        await waitForReady(store, rql, optional);
        await client.command(`insert ${name} [{ id: 1, value: 7 }]`, null, []);
        await waitForRows(store, rql, optional, 1);
        expect(rowsOf(store, rql, optional)[0].value.isSome()).toBe(true);

        await client.command(`update ${name} { value: none } filter id == 1`, null, []);
        await waitFor(store, () => rowsOf(store, rql, optional)[0]?.value?.isNone() === true);
        release();
    });

    it('an UPDATE that sets the column flips None to Some on the diff path', async () => {
        const name = await optionalTable('set');
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, optional);
        await waitForReady(store, rql, optional);
        await client.command(`insert ${name} [{ id: 1, value: none }]`, null, []);
        await waitForRows(store, rql, optional, 1);
        expect(rowsOf(store, rql, optional)[0].value.isNone()).toBe(true);

        await client.command(`update ${name} { value: 9 } filter id == 1`, null, []);
        await waitFor(store, () => rowsOf(store, rql, optional)[0]?.value?.isSome() === true);
        expect(rowsOf(store, rql, optional)[0].value.unwrap()).toBe(9);
        release();
    });

    it('an optional utf8 column tells an absent value apart from an empty string', async () => {
        const name = `${ns}::${tableName(`optstr_${format}`)}`;
        await createTable(client, name, 'id: int4, value: Option(utf8)');
        const shape = Shape.object({id: Shape.int4(), value: Shape.option(Shape.utf8())});
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, shape);
        await waitForReady(store, rql, shape);
        await client.command(`insert ${name} [{ id: 1, value: '' }, { id: 2, value: none }]`, null, []);
        await waitForRows(store, rql, shape, 2);
        const byId = new Map(rowsOf(store, rql, shape).map(row => [row.id, row.value]));
        expect(byId.get(1)!.isSome()).toBe(true);
        expect(byId.get(1)!.unwrap()).toBe('');
        expect(byId.get(2)!.isNone()).toBe(true);
        release();
    });
});
