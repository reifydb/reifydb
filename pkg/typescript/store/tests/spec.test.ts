// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {DurationValue, Shape} from '@reifydb/core';
import {Store, rql} from '../src';
import {BatchingFakeClient, FakeClient, flush} from './fake-client';

const item = Shape.object({id: Shape.int4(), name: Shape.utf8()});
const total = Shape.object({total: Shape.int8()});
const config = {linger: DurationValue.parse('5s')};

describe('store specs', () => {
    it('two specs built separately with the same rql, params and shape share one entry and one subscription', async () => {
        // Hooks build their spec at module scope in different files, so identity must not be what keys the entry.
        const client = new FakeClient();
        const store = new Store(client);
        const first = rql(item)<{id: number}>`from test::items filter { id == $id }`;
        const second = rql(item)<{id: number}>`from test::items filter { id == $id }`;
        store.subscribe(first, {id: 1});
        store.subscribe(second, {id: 1});
        await flush();
        expect(client.subscribes).toHaveLength(1);
        client.subscribes[0].resolve('sub-1');
        await flush();
        expect(store.getEntry(second, {id: 1})).toBe(store.getEntry(first, {id: 1}));
        expect(store.getEntry(second, {id: 1}).status).toBe('ready');
    });

    it('the same rql and params with a different shape get their own entry', () => {
        // One entry for two shapes would hand rows decoded for one shape to a caller typed by the other.
        const client = new FakeClient();
        const store = new Store(client);
        store.subscribe(rql(item)`from test::items`, null);
        store.subscribe(rql(total)`from test::items`, null);
        expect(client.subscribes).toHaveLength(2);
        expect(Object.keys(store.getSnapshot().entries)).toHaveLength(2);
    });

    it('a one shape spec and a one element tuple spec get their own entry', () => {
        // Their data differs in shape (rows versus one array per frame), so sharing would break one of the two readers.
        const client = new FakeClient();
        const store = new Store(client);
        store.query(rql(item)`from test::items`, null);
        store.query(rql([item])`from test::items`, null);
        expect(client.queries).toHaveLength(2);
        expect(Object.keys(store.getSnapshot().entries)).toHaveLength(2);
    });

    it('subscribe hands the spec rql, params, shape and config to the client unchanged', () => {
        // A dropped config silently falls back to server defaults, so hydration and linger settings would never apply.
        const client = new FakeClient();
        const store = new Store(client);
        const spec = rql(item)<{id: number}>`from test::items filter { id == $id }`.options({config});
        store.subscribe(spec, {id: 7});
        expect(client.subscribes[0].rql).toBe('from test::items filter { id == $id }');
        expect(client.subscribes[0].params).toEqual({id: 7});
        expect(client.subscribes[0].shape).toBe(item);
        expect(client.subscribes[0].config).toBe(config);
    });

    it('a batched subscribe hands the spec rql, params, shape and config to the batch request unchanged', async () => {
        // The batch path builds its own request, so it can lose the config even when the direct path keeps it.
        const client = new BatchingFakeClient();
        const store = new Store(client, {batch: true});
        const spec = rql(item)<{id: number}>`from test::items filter { id == $id }`.options({config});
        store.subscribe(spec, {id: 7});
        await flush();
        const request = client.batches[0].subscriptions[0];
        expect(request.rql).toBe('from test::items filter { id == $id }');
        expect(request.params).toEqual({id: 7});
        expect(request.shape).toBe(item);
        expect(request.config).toBe(config);
    });

    it('query sends a one shape spec as a one element tuple and a tuple spec as the tuple itself', () => {
        // The client pairs frames with shapes by position, so the shapes it gets must be exactly the frames asked for.
        const client = new FakeClient();
        const store = new Store(client);
        store.query(rql(item)<{id: number}>`from test::items filter { id == $id }`, {id: 3});
        store.query(rql([item, total])`output from test::items; output from test::totals`, null);
        expect(client.queries[0].rql).toBe('from test::items filter { id == $id }');
        expect(client.queries[0].params).toEqual({id: 3});
        expect(client.queries[0].shapes).toEqual([item]);
        expect(client.queries[1].rql).toBe('output from test::items; output from test::totals');
        expect(client.queries[1].params).toBeNull();
        expect(client.queries[1].shapes).toEqual([item, total]);
    });

    it('command and admin hand the spec rql, params and shapes to their own channel unchanged', () => {
        // The channel picks the rights, so a write spec on the wrong channel would run with the wrong ones.
        const client = new FakeClient();
        const store = new Store(client);
        const spec = rql.write([item, total])<{id: number}>`insert test::items [{ id: $id }]; output from test::items; output from test::totals`;
        store.command(spec, {id: 4});
        store.admin(spec, {id: 5});
        expect(client.commands).toHaveLength(1);
        expect(client.admins).toHaveLength(1);
        expect(client.queries).toHaveLength(0);
        expect(client.commands[0].rql).toBe(spec.rql);
        expect(client.commands[0].params).toEqual({id: 4});
        expect(client.commands[0].shapes).toEqual([item, total]);
        expect(client.admins[0].rql).toBe(spec.rql);
        expect(client.admins[0].params).toEqual({id: 5});
        expect(client.admins[0].shapes).toEqual([item, total]);
    });

    it('an unseen tuple spec reads as loading with one empty array per frame, the same object every time', () => {
        // useSyncExternalStore loops forever on a snapshot that changes identity between two reads.
        const store = new Store(new FakeClient());
        const spec = rql([item, total])`output from test::items; output from test::totals`;
        const entry = store.getEntry(spec, null);
        expect(entry.status).toBe('loading');
        expect(entry.data).toEqual([[], []]);
        expect(entry.rows.map(rows => rows.size)).toEqual([0, 0]);
        expect(store.getEntry(spec, null)).toBe(entry);
        expect(store.getEntry(rql([item, total])`output from test::other; output from test::totals`, null)).toBe(entry);
    });

    it('a tuple query fills one rows map and one data array per frame and marks the entry ready', async () => {
        // Frames must land by position, or a component reading data[1] sees rows of the first frame.
        const client = new FakeClient();
        const store = new Store(client);
        const spec = rql([item, total])`output from test::items; output from test::totals`;
        const promise = store.query(spec, null);
        expect(store.getEntry(spec, null).data).toEqual([[], []]);
        client.queries[0].resolve([[{'#rownum': 4, id: 1, name: 'a'}, {'#rownum': 8, id: 2, name: 'b'}], [{'#rownum': 1, total: 2n}]]);
        await promise;
        const entry = store.getEntry(spec, null);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual([[{id: 1, name: 'a'}, {id: 2, name: 'b'}], [{total: 2n}]]);
        expect(entry.rows.map((rows: ReadonlyMap<number, unknown>) => Array.from(rows.entries()))).toEqual([
            [[4, {id: 1, name: 'a'}], [8, {id: 2, name: 'b'}]],
            [[1, {total: 2n}]],
        ]);
    });

    it('a failed tuple query marks the entry error and keeps one data array per frame', async () => {
        // A reader indexes data by frame even on error, so the error entry must keep the tuple layout.
        const client = new FakeClient();
        const store = new Store(client);
        const spec = rql([item, total])`output from test::items; output from test::totals`;
        const promise = store.query(spec, null);
        const error = new Error('no such table');
        client.queries[0].reject(error);
        await expect(promise).rejects.toBe(error);
        const entry = store.getEntry(spec, null);
        expect(entry.status).toBe('error');
        expect(entry.error).toBe(error);
        expect(entry.data).toEqual([[], []]);
        expect(entry.rows.map(rows => rows.size)).toEqual([0, 0]);
    });

    it('a write spec with no shapes resolves to an empty tuple on command and admin even when frames come back', async () => {
        // With no shapes the client skips the frame check, so any frame passed on would be unchecked and untyped.
        const client = new FakeClient();
        const store = new Store(client);
        const spec = rql.write([])`insert test::items [{ id: 1, name: 'a' }]`;
        const command = store.command(spec, null);
        const admin = store.admin(spec, null);
        client.commands[0].resolve([[{inserted: 1}]]);
        client.admins[0].resolve([[{inserted: 1}]]);
        expect(await command).toEqual([]);
        expect(await admin).toEqual([]);
    });
});
