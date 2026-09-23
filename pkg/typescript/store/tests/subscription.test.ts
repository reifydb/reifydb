// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store, rql} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const items = rql(shape)`from test::items`;

function setup() {
    const client = new FakeClient();
    const errors: Error[] = [];
    const store = new Store(client, {onBackgroundError: error => errors.push(error)});
    return {client, store, errors};
}

describe('subscription lifecycle', () => {
    it('first subscribe creates a loading entry and calls the client once', () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        expect(store.getEntry(items, null).status).toBe('loading');
        expect(client.subscribes).toHaveLength(1);
        expect(client.subscribes[0].rql).toBe(items.rql);
        expect(client.subscribes[0].shape).toBe(shape);
    });

    it('the entry turns ready when the subscribe promise resolves', async () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        expect(store.getEntry(items, null).status).toBe('ready');
    });

    it('a rejected subscribe turns the entry to error carrying the rejection', async () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        const error = new Error('denied');
        client.subscribes[0].reject(error);
        await flush();
        const entry = store.getEntry(items, null);
        expect(entry.status).toBe('error');
        expect(entry.error).toBe(error);
    });

    it('a change the client cannot decode marks the entry error instead of leaving it silently stale', async () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        const error = new Error('column "name": expected Utf8, got Int4');
        client.subscribes[0].callbacks.onError?.(error);
        const entry = store.getEntry(items, null);
        expect(entry.status).toBe('error');
        expect(entry.error).toBe(error);
    });

    it('an error reported after release does not resurrect the closed entry', async () => {
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        release();
        await flush();
        client.subscribes[0].callbacks.onError?.(new Error('too late'));
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('rows pushed before the acknowledgement are merged, not lost', async () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        expect(store.getEntry(items, null).data).toEqual([{id: 1, name: 'a'}]);
        client.subscribes[0].resolve('sub-1');
        await flush();
        const entry = store.getEntry(items, null);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual([{id: 1, name: 'a'}]);
    });

    it('a second subscribe on the same key does not call the client again and both see the same rows', () => {
        const {client, store} = setup();
        const spec = rql(shape)<{a: number; b: number}>`from test::items`;
        store.subscribe(spec, {a: 1, b: 2});
        store.subscribe(spec, {b: 2, a: 1});
        expect(client.subscribes).toHaveLength(1);
        client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        expect(store.getEntry(spec, {a: 1, b: 2}).data).toEqual([{id: 1, name: 'a'}]);
        expect(store.getEntry(spec, {b: 2, a: 1}).data).toBe(store.getEntry(spec, {a: 1, b: 2}).data);
    });

    it('release decrements and the last release calls unsubscribe and deletes the entry', async () => {
        const {client, store} = setup();
        const first = store.subscribe(items, null);
        const second = store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        first();
        await flush();
        expect(client.unsubscribes).toHaveLength(0);
        expect(store.getSnapshot().entries).not.toEqual({});
        second();
        await flush();
        expect(client.unsubscribes).toHaveLength(1);
        expect(client.unsubscribes[0].subscriptionId).toBe('sub-1');
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('release twice on the same handle unsubscribes once', async () => {
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        release();
        release();
        await flush();
        expect(client.unsubscribes).toHaveLength(0);
        expect(store.getEntry(items, null).status).toBe('ready');
    });

    it('release followed by subscribe on the same key in the same microtask does not unsubscribe', async () => {
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        release();
        store.subscribe(items, null);
        await flush();
        expect(client.unsubscribes).toHaveLength(0);
        expect(client.subscribes).toHaveLength(1);
        expect(store.getEntry(items, null).status).toBe('ready');
    });

    it('an unsubscribe rejection reaches the onBackgroundError callback', async () => {
        const {client, store, errors} = setup();
        const release = store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        release();
        await flush();
        const error = new Error('socket closed');
        client.unsubscribes[0].reject(error);
        await flush();
        expect(errors).toEqual([error]);
    });

    it('callbacks arriving after release are dropped', async () => {
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        client.subscribes[0].resolve('sub-1');
        await flush();
        release();
        await flush();
        client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('a release before the acknowledgement unsubscribes once the id arrives', async () => {
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        release();
        await flush();
        expect(client.unsubscribes).toHaveLength(0);
        client.subscribes[0].resolve('sub-1');
        await flush();
        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1']);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('update replaces and remove drops rows delivered through the callbacks', () => {
        const {client, store} = setup();
        store.subscribe(items, null);
        const {callbacks} = client.subscribes[0];
        callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}, {'#rownum': 2, id: 2, name: 'b'}]);
        callbacks.onUpdate?.([{'#rownum': 1, id: 1, name: 'a2'}]);
        expect(store.getEntry(items, null).data).toEqual([{id: 1, name: 'a2'}, {id: 2, name: 'b'}]);
        callbacks.onRemove?.([{'#rownum': 2, id: 2, name: 'b'}]);
        expect(store.getEntry(items, null).data).toEqual([{id: 1, name: 'a2'}]);
    });
});
