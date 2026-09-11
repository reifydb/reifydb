// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items';

describe('seeding and reset', () => {
    it('seed marks the entry ready with the given rows', () => {
        const store = new Store(new FakeClient());
        store.seed(rql, null, shape, [{id: 1, name: 'a'}]);
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('ready');
        expect(entry.data).toEqual([{id: 1, name: 'a'}]);
    });

    it('subscribe on a seeded key never calls the client', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.seed(rql, null, shape, [{id: 1, name: 'a'}]);
        store.subscribe(rql, null, shape);
        await flush();
        expect(client.subscribes).toHaveLength(0);
        expect(store.getEntry(rql, null, shape).data).toEqual([{id: 1, name: 'a'}]);
    });

    it('release on a seeded entry does not delete it', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.seed(rql, null, shape, [{id: 1, name: 'a'}]);
        const release = store.subscribe(rql, null, shape);
        release();
        await flush();
        expect(client.unsubscribes).toHaveLength(0);
        expect(store.getEntry(rql, null, shape).status).toBe('ready');
    });

    it('fail marks the entry error', () => {
        const store = new Store(new FakeClient());
        const error = new Error('injected');
        store.fail(rql, null, shape, error);
        const entry = store.getEntry(rql, null, shape);
        expect(entry.status).toBe('error');
        expect(entry.error).toBe(error);
    });

    it('reset unsubscribes every live subscription and clears all entries', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.subscribe(rql, null, shape);
        store.subscribe(rql + ' filter id == 1', null, shape);
        store.seed('from test::seeded', null, shape, []);
        client.subscribes[0].resolve('sub-1');
        client.subscribes[1].resolve('sub-2');
        await flush();
        store.reset();
        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1', 'sub-2']);
        expect(store.getSnapshot().entries).toEqual({});
        store.subscribe(rql, null, shape);
        expect(client.subscribes).toHaveLength(3);
    });
});
