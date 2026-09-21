// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store, rql} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const items = rql(shape)`from test::items`;

describe('reset', () => {
    it('reset unsubscribes every live subscription and clears all entries', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.subscribe(items, null);
        store.subscribe(rql(shape)`from test::items filter id == 1`, null);
        client.subscribes[0].resolve('sub-1');
        client.subscribes[1].resolve('sub-2');
        await flush();
        store.reset();
        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1', 'sub-2']);
        expect(store.getSnapshot().entries).toEqual({});
        store.subscribe(items, null);
        expect(client.subscribes).toHaveLength(3);
    });
});
