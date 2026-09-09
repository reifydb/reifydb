// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '../../src';
import {connect, namespace, poll, waitFor} from './setup';
import {createTable, rowsOf, tableName, waitForReady, waitForRows} from './subscription-helpers';

const ns = namespace('sub_life');
const items = Shape.object({id: Shape.int4(), name: Shape.utf8()});
const subscriptionRow = Shape.object({id: Shape.uint8()});

describe('subscription lifecycle against a live server', () => {
    let client: WsClient;
    let store: Store;

    async function table(prefix: string, columns = 'id: int4, name: utf8'): Promise<string> {
        const name = `${ns}::${tableName(prefix)}`;
        await createTable(client, name, columns);
        return name;
    }

    async function subscriptionCount(): Promise<number> {
        const [rows] = await client.query('from system::subscriptions', null, [subscriptionRow]);
        return rows.length;
    }

    beforeAll(async () => {
        client = await connect('rbcf');
        await client.admin(`create namespace ${ns}`, null, []);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    describe('initial state', () => {
        it('an entry that was never subscribed reads as loading with no rows and no error', () => {
            const entry = store.getEntry(`from ${ns}::never_subscribed`, null, items);
            expect(entry.status).toBe('loading');
            expect(entry.data).toEqual([]);
            expect(entry.rows.size).toBe(0);
            expect(entry.error).toBeUndefined();
        });

        it('a fresh subscribe is loading before the server acknowledges', async () => {
            const rql = `from ${await table('initial')}`;
            const release = store.subscribe(rql, null, items);
            expect(store.getEntry(rql, null, items).status).toBe('loading');
            await waitForReady(store, rql, items);
            release();
        });
    });

    describe('subscription lifecycle', () => {
        it('subscribing registers exactly one subscription on the server', async () => {
            const rql = `from ${await table('register')}`;
            const before = await subscriptionCount();
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            expect(await subscriptionCount()).toBe(before + 1);
            release();
            await poll(async () => (await subscriptionCount()) === before);
        });

        it('releasing removes the server subscription and stops further callbacks reaching the entry', async () => {
            const name = await table('release');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rql, items, 1);

            release();
            await poll(async () => store.getEntry(rql, null, items).status === 'loading');
            await client.command(`insert ${name} [{ id: 2, name: 'b' }]`, null, []);
            await new Promise(resolve => setTimeout(resolve, 300));
            expect(rowsOf(store, rql, items)).toEqual([]);
        });

        it('releasing drops the entry, so rows are not served stale to the next reader', async () => {
            const name = await table('drop');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rql, items, 1);
            release();
            await poll(async () => store.getEntry(rql, null, items).status === 'loading');
            expect(store.getEntry(rql, null, items).data).toEqual([]);
        });

        it('a second subscribe after a full release opens a new server subscription', async () => {
            const rql = `from ${await table('reopen')}`;
            const before = await subscriptionCount();
            const first = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            first();
            await poll(async () => (await subscriptionCount()) === before);
            const second = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            expect(await subscriptionCount()).toBe(before + 1);
            second();
            await poll(async () => (await subscriptionCount()) === before);
        });
    });

    describe('operation callbacks', () => {
        it('an INSERT reaches the entry', async () => {
            const name = await table('insert');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rql, items, 1);
            expect(rowsOf(store, rql, items)).toEqual([{id: 1, name: 'a'}]);
            release();
        });

        it('an UPDATE replaces the row in place rather than adding a second one', async () => {
            const name = await table('update');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rql, items, 1);
            const [rownum] = Array.from(store.getEntry(rql, null, items).rows.keys());

            await client.command(`update ${name} { name: 'b' } filter id == 1`, null, []);
            await waitFor(store, () => rowsOf(store, rql, items)[0]?.name === 'b');
            expect(rowsOf(store, rql, items)).toHaveLength(1);
            expect(Array.from(store.getEntry(rql, null, items).rows.keys())).toEqual([rownum]);
            release();
        });

        it('a REMOVE drops the row', async () => {
            const name = await table('remove');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rql, items, 1);
            await client.command(`delete ${name} filter id == 1`, null, []);
            await waitForRows(store, rql, items, 0);
            release();
        });

        it('insert, update and remove arriving in sequence leave the entry consistent', async () => {
            const name = await table('sequence');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);

            await client.command(`insert ${name} [{ id: 1, name: 'a' }, { id: 2, name: 'b' }]`, null, []);
            await waitForRows(store, rql, items, 2);
            await client.command(`update ${name} { name: 'z' } filter id == 1`, null, []);
            await waitFor(store, () => rowsOf(store, rql, items).some(row => row.name === 'z'));
            await client.command(`delete ${name} filter id == 2`, null, []);
            await waitForRows(store, rql, items, 1);
            expect(rowsOf(store, rql, items)).toEqual([{id: 1, name: 'z'}]);
            release();
        });

        it('a batch of rows in one statement all reach the entry', async () => {
            const name = await table('batch');
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            const rows = Array.from({length: 25}, (_, i) => `{ id: ${i}, name: 'n${i}' }`).join(', ');
            await client.command(`insert ${name} [${rows}]`, null, []);
            await waitForRows(store, rql, items, 25);
            expect(rowsOf(store, rql, items).map(row => row.id).sort((a, b) => a - b)).toEqual(
                Array.from({length: 25}, (_, i) => i)
            );
            release();
        });
    });

    describe('error handling', () => {
        it('a syntactically invalid query turns the entry to error and never to ready', async () => {
            const rql = 'this is not rql';
            store.subscribe(rql, null, items);
            await waitFor(store, () => store.getEntry(rql, null, items).status === 'error');
            const entry = store.getEntry(rql, null, items);
            expect(entry.error).toBeInstanceOf(Error);
            expect(entry.data).toEqual([]);
        });

        it('a subscription to a table that does not exist turns the entry to error', async () => {
            const rql = `from ${ns}::definitely_missing`;
            store.subscribe(rql, null, items);
            await waitFor(store, () => store.getEntry(rql, null, items).status === 'error');
            expect(store.getEntry(rql, null, items).error).toBeInstanceOf(Error);
        });

        it('a failed subscription does not register on the server', async () => {
            const before = await subscriptionCount();
            const rql = `from ${ns}::also_missing`;
            store.subscribe(rql, null, items);
            await waitFor(store, () => store.getEntry(rql, null, items).status === 'error');
            expect(await subscriptionCount()).toBe(before);
        });
    });

    describe('edge cases', () => {
        it('two subscriptions on different queries stay independent', async () => {
            const first = await table('indep_a');
            const second = await table('indep_b');
            const rqlA = `from ${first}`;
            const rqlB = `from ${second}`;
            const releaseA = store.subscribe(rqlA, null, items);
            const releaseB = store.subscribe(rqlB, null, items);
            await waitForReady(store, rqlA, items);
            await waitForReady(store, rqlB, items);

            await client.command(`insert ${first} [{ id: 1, name: 'a' }]`, null, []);
            await waitForRows(store, rqlA, items, 1);
            expect(rowsOf(store, rqlB, items)).toEqual([]);
            releaseA();
            releaseB();
        });

        it('a subscription over an empty table becomes ready with no rows', async () => {
            const rql = `from ${await table('empty')}`;
            const release = store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            expect(rowsOf(store, rql, items)).toEqual([]);
            release();
        });

        it('reset clears every entry and unsubscribes everything it opened', async () => {
            const rql = `from ${await table('reset')}`;
            const before = await subscriptionCount();
            store.subscribe(rql, null, items);
            await waitForReady(store, rql, items);
            store.reset();
            expect(store.getSnapshot().entries).toEqual({});
            await poll(async () => (await subscriptionCount()) === before);
        });
    });
});
