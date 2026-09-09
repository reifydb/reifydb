// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '../../src';
import {connect, namespace, poll, waitFor} from './setup';

const ns = namespace('store_it');
const items = Shape.object({id: Shape.int4(), name: Shape.string()});
const subscriptionRow = Shape.object({id: Shape.uint8()});

async function subscriptionCount(client: WsClient): Promise<number> {
    const [rows] = await client.query('from system::subscriptions', null, [subscriptionRow]);
    return rows.length;
}

describe.each(['frames', 'rbcf'] as const)('store against a live server (%s)', format => {
    let client: WsClient;
    let store: Store;
    const table = `${ns}::items_${format}`;

    beforeAll(async () => {
        client = await connect(format);
        await client.admin(`create namespace ${ns}`, null, []).catch(() => undefined);
        await client.admin(`create table ${table} { id: int4, name: utf8 }`, null, []);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    it('insert through command arrives with a rownum, update replaces it and delete removes it', async () => {
        const rql = `from ${table}`;
        const release = store.subscribe(rql, null, items);
        await waitFor(store, () => store.getEntry(rql, null, items).status === 'ready');

        await store.command(`insert ${table} [{ id: 1, name: 'a' }]`, null, []);
        await waitFor(store, () => store.getEntry(rql, null, items).data.length === 1);
        const entry = store.getEntry(rql, null, items);
        const [rownum] = Array.from(entry.rows.keys());
        expect(rownum).toEqual(expect.any(Number));
        expect(entry.data).toEqual([{id: 1, name: 'a'}]);

        await store.command(`update ${table} { name: 'b' } filter id == 1`, null, []);
        await waitFor(store, () => store.getEntry(rql, null, items).data[0]?.name === 'b');
        expect(Array.from(store.getEntry(rql, null, items).rows.keys())).toEqual([rownum]);

        await store.command(`delete ${table} filter id == 1`, null, []);
        await waitFor(store, () => store.getEntry(rql, null, items).data.length === 0);
        release();
    });

    it('admin runs DDL that a later command and query can use, so it reached the server with admin rights', async () => {
        const created = `${ns}::created_${format}`;
        await store.admin(`create table ${created} { id: int4, name: utf8 }`, null, []);
        await store.command(`insert ${created} [{ id: 1, name: 'a' }]`, null, []);
        const [rows] = await client.query(`from ${created}`, null, [items]);
        expect(rows).toEqual([{id: 1, name: 'a'}]);
    });

    it('admin rejects a DDL statement the server refuses rather than reporting success', async () => {
        await expect(store.admin(`create table ${table} { id: int4 }`, null, [])).rejects.toThrow();
    });

    it('admin reads a system table and types it by the given shape', async () => {
        const [rows] = await store.admin('from system::subscriptions', null, [subscriptionRow]);
        expect(Array.isArray(rows)).toBe(true);
    });

    it('two subscribers on the same rql share one server subscription', async () => {
        const rql = `from ${table} filter id > 100`;
        const before = await subscriptionCount(client);
        const first = store.subscribe(rql, null, items);
        const second = store.subscribe(rql, null, items);
        await waitFor(store, () => store.getEntry(rql, null, items).status === 'ready');
        expect(await subscriptionCount(client)).toBe(before + 1);

        first();
        second();
        await poll(async () => (await subscriptionCount(client)) === before);
    });

    it('releasing the last subscriber removes it from system::subscriptions', async () => {
        const rql = `from ${table} filter id > 200`;
        const before = await subscriptionCount(client);
        const release = store.subscribe(rql, null, items);
        await waitFor(store, () => store.getEntry(rql, null, items).status === 'ready');
        expect(await subscriptionCount(client)).toBe(before + 1);
        release();
        await poll(async () => (await subscriptionCount(client)) === before);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('a command with two output statements returns two frames typed by two shapes', async () => {
        const count = Shape.object({n: Shape.int8()});
        const [rows, counts] = await store.command(
            `insert ${table} [{ id: 10, name: 'ten' }]; output from ${table} filter id == 10; output from ${table} filter id == 10 aggregate { n: math::count(id) }`,
            null,
            [items, count]
        );
        expect(rows).toEqual([{id: 10, name: 'ten'}]);
        expect(counts).toEqual([{n: 1n}]);
    });

    it('a command whose second statement fails leaves the first statement write absent', async () => {
        await expect(
            store.command(`insert ${table} [{ id: 11, name: 'eleven' }]; output from ${ns}::missing`, null, [items])
        ).rejects.toThrow();
        const [rows] = await client.query(`from ${table} filter id == 11`, null, [items]);
        expect(rows).toEqual([]);
    });

    it('query reads rows typed by the shape and caches them on the entry', async () => {
        const queried = `${ns}::queried_${format}`;
        await store.admin(`create table ${queried} { id: int4, name: utf8 }`, null, []);
        await store.command(`insert ${queried} [{ id: 1, name: 'a' }, { id: 2, name: 'b' }]`, null, []);
        const rql = `from ${queried} sort { id: ASC }`;

        const rows = await store.query(rql, null, items);

        expect(rows).toEqual([{id: 1, name: 'a'}, {id: 2, name: 'b'}]);
        expect(typeof rows[0].id).toBe('number');
        expect(typeof rows[0].name).toBe('string');
        const entry = store.getEntry(rql, null, items);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual(rows);
    });

    it('a query after a command sees the write, so the cached entry is refreshed rather than reused', async () => {
        const refreshed = `${ns}::refreshed_${format}`;
        await store.admin(`create table ${refreshed} { id: int4, name: utf8 }`, null, []);
        const rql = `from ${refreshed}`;
        expect(await store.query(rql, null, items)).toEqual([]);

        await store.command(`insert ${refreshed} [{ id: 7, name: 'seven' }]`, null, []);

        expect(await store.query(rql, null, items)).toEqual([{id: 7, name: 'seven'}]);
        expect(store.getEntry(rql, null, items).data).toEqual([{id: 7, name: 'seven'}]);
    });

    it('query rejects a statement the server refuses and records the failure on the entry', async () => {
        const rql = `from ${ns}::absent_${format}`;

        await expect(store.query(rql, null, items)).rejects.toThrow();

        const entry = store.getEntry(rql, null, items);
        expect(entry.status).toBe('error');
        expect(entry.error).toBeInstanceOf(Error);
    });

    it('command rejects a statement the server refuses and creates no entry', async () => {
        const rql = `insert ${ns}::absent_${format} [{ id: 1, name: 'a' }]`;
        const before = Object.keys(store.getSnapshot().entries);

        await expect(store.command(rql, null, [])).rejects.toThrow();

        expect(Object.keys(store.getSnapshot().entries)).toEqual(before);
    });

    it('a command that writes and outputs returns the written rows typed by the shape', async () => {
        const written = `${ns}::written_${format}`;
        await store.admin(`create table ${written} { id: int4, name: utf8 }`, null, []);

        const [rows] = await store.command(
            `insert ${written} [{ id: 3, name: 'three' }]; output from ${written}`,
            null,
            [items]
        );

        expect(rows).toEqual([{id: 3, name: 'three'}]);
        expect(typeof rows[0].id).toBe('number');
    });

    it('command cannot run DDL, so the admin channel is not just a different name for it', async () => {
        await expect(store.command(`create table ${ns}::via_command_${format} { id: int4 }`, null, [])).rejects.toThrow();
    });

    it('a shape with a wrong column type against real data throws ShapeMismatch', async () => {
        const wrong = Shape.object({id: Shape.string(), name: Shape.string()});
        await store.command(`insert ${table} [{ id: 12, name: 'twelve' }]`, null, []);
        const promise = store.query(`from ${table} filter id == 12`, null, wrong);
        await expect(promise).rejects.toMatchObject({name: 'ShapeMismatch'});
    });
});
