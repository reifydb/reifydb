// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const other = Shape.object({total: Shape.int8()});
const rql = 'from test::items';

describe('query', () => {
    it('creates a loading entry, then replaces its rows with the result keyed by index and marks ready', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.query(rql, null, shape);
        expect(store.getEntry(rql, null, shape).status).toBe('loading');
        expect(client.queries[0].shapes).toEqual([shape]);
        client.queries[0].resolve([[{id: 1, name: 'a'}, {id: 2, name: 'b'}]]);
        expect(await promise).toEqual([{id: 1, name: 'a'}, {id: 2, name: 'b'}]);
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('ready');
        expect(Array.from(entry.rows.entries())).toEqual([[0, {id: 1, name: 'a'}], [1, {id: 2, name: 'b'}]]);
        expect(entry.data).toEqual([{id: 1, name: 'a'}, {id: 2, name: 'b'}]);
    });

    it('query sends the shape to the client as a one element shapes tuple and passes rql and params through', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.query(rql, {a: 1, b: 'two'}, shape);
        expect(client.queries[0].rql).toBe(rql);
        expect(client.queries[0].params).toEqual({a: 1, b: 'two'});
        expect(client.queries[0].shapes).toEqual([shape]);
    });

    it('query uses the query channel, not the command or admin channel', () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.query(rql, null, shape);
        expect(client.queries).toHaveLength(1);
        expect(client.commands).toHaveLength(0);
        expect(client.admins).toHaveLength(0);
    });

    it('a second query on the same key replaces rows entirely rather than merging', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const first = store.query(rql, null, shape);
        client.queries[0].resolve([[{id: 1, name: 'a'}, {id: 2, name: 'b'}]]);
        await first;
        const second = store.query(rql, null, shape);
        client.queries[1].resolve([[{id: 3, name: 'c'}]]);
        await second;
        expect(store.getEntry(rql, null, shape).data).toEqual([{id: 3, name: 'c'}]);
    });

    it('a rejected query marks the entry error and rejects the promise', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.query(rql, null, shape);
        const error = new Error('no such table');
        client.queries[0].reject(error);
        await expect(promise).rejects.toBe(error);
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('error');
        expect(entry.error).toBe(error);
    });

    it('command passes rql, params and shapes through, returns the tuple and creates no entry', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.command('insert test::items [{ id: 1 }]; output from test::items; output from test::totals', {a: 1}, [shape, other]);
        expect(client.commands[0].rql).toBe('insert test::items [{ id: 1 }]; output from test::items; output from test::totals');
        expect(client.commands[0].params).toEqual({a: 1});
        expect(client.commands[0].shapes).toEqual([shape, other]);
        expect(client.queries).toHaveLength(0);
        client.commands[0].resolve([[{id: 1, name: 'a'}], [{total: 1n}]]);
        const [items, totals] = await promise;
        expect(items).toEqual([{id: 1, name: 'a'}]);
        expect(totals).toEqual([{total: 1n}]);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('command passes through and creates no entry', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.command('insert test::items [{ id: 1 }]', null, []);
        expect(client.commands[0].rql).toBe('insert test::items [{ id: 1 }]');
        expect(client.queries).toHaveLength(0);
        client.commands[0].resolve([]);
        expect(await promise).toEqual([]);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('admin passes through to the admin channel and creates no entry', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.admin('create table test::items { id: int4 }', {a: 1}, [shape]);
        expect(client.admins[0].rql).toBe('create table test::items { id: int4 }');
        expect(client.admins[0].params).toEqual({a: 1});
        expect(client.admins[0].shapes).toEqual([shape]);
        expect(client.commands).toHaveLength(0);
        expect(client.queries).toHaveLength(0);
        client.admins[0].resolve([[]]);
        expect(await promise).toEqual([[]]);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('an admin rejection propagates and leaves no entry', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.admin('create table test::items { id: int4 }', null, []);
        const error = new Error('already exists');
        client.admins[0].reject(error);
        await expect(promise).rejects.toBe(error);
        await flush();
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('a command leaves the rows an earlier query cached untouched', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const query = store.query(rql, null, shape);
        client.queries[0].resolve([[{id: 1, name: 'a'}]]);
        await query;
        const command = store.command(`insert test::items [{ id: 2 }]`, null, []);
        client.commands[0].resolve([]);
        await command;
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual([{id: 1, name: 'a'}]);
    });

    it('an admin leaves the rows an earlier query cached untouched', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const query = store.query(rql, null, shape);
        client.queries[0].resolve([[{id: 1, name: 'a'}]]);
        await query;
        const admin = store.admin('create table test::other { id: int4 }', null, []);
        client.admins[0].resolve([]);
        await admin;
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual([{id: 1, name: 'a'}]);
    });

    it('a command rejection propagates and leaves no entry', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const promise = store.command('insert test::items [{ id: 1 }]', null, []);
        const error = new Error('constraint');
        client.commands[0].reject(error);
        await expect(promise).rejects.toBe(error);
        await flush();
        expect(store.getSnapshot().entries).toEqual({});
    });
});
