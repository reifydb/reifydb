// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store} from '../src';
import {BatchingFakeClient, FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const monitors = 'from test::monitors';
const results = 'from test::results';
const regions = 'from test::regions';

function batching() {
    const client = new BatchingFakeClient();
    const errors: Error[] = [];
    const store = new Store(client, {batch: true, onBackgroundError: error => errors.push(error)});
    return {client, store, errors};
}

describe('batched subscription opening', () => {
    it('leaves every subscribe on its own unless the store was told to batch', async () => {
        // Batching changes when a subscription reaches the transport and how a failure fans out, so
        // a store that was not asked for it must keep issuing one call per subscription.
        const client = new BatchingFakeClient();
        const store = new Store(client);
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();
        expect(client.batches).toHaveLength(0);
        expect(client.subscribes).toHaveLength(2);
    });

    it('opens every subscription started in one tick as a single batch', async () => {
        // React runs a page's mount effects in one synchronous pass, so this is the whole point:
        // six hooks on a dashboard become one round trip rather than six.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        store.subscribe(regions, null, shape);
        await flush();

        expect(client.subscribes).toHaveLength(0);
        expect(client.batches).toHaveLength(1);
        expect(client.batches[0].members.map(member => member.rql)).toEqual([monitors, results, regions]);
    });

    it('turns each entry ready with the id the ack gave that member', async () => {
        // The ack reports ids positionally against the members that were sent. Reading them in the
        // wrong order would leave every entry ready and every later unsubscribe aimed elsewhere.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();

        client.batches[0].resolve({batchId: 'batch-1', subscriptionIds: ['sub-monitors', 'sub-results']});
        await flush();

        expect(store.getEntry(monitors, null, shape).status).toBe('ready');
        expect(store.getEntry(results, null, shape).status).toBe('ready');

        store.reset();
        expect(client.unsubscribes.map(call => call.subscriptionId).sort()).toEqual([
            'sub-monitors',
            'sub-results'
        ]);
    });

    it('marks every member in error when the batch itself is refused', async () => {
        // One ack covers all of them, so a refusal is not one subscription's problem. An entry left
        // loading would render as a spinner that never resolves.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();

        const error = new Error('denied');
        client.batches[0].reject(error);
        await flush();

        expect(store.getEntry(monitors, null, shape).status).toBe('error');
        expect(store.getEntry(monitors, null, shape).error).toBe(error);
        expect(store.getEntry(results, null, shape).status).toBe('error');
    });

    it('marks a member in error when the ack names no id for it', async () => {
        // A short ack is the one failure that looks like success. Without this the entry would stay
        // loading for the life of the page with nothing recorded anywhere.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();

        client.batches[0].resolve({batchId: 'batch-1', subscriptionIds: ['sub-monitors']});
        await flush();

        expect(store.getEntry(monitors, null, shape).status).toBe('ready');
        expect(store.getEntry(results, null, shape).status).toBe('error');
    });

    it('unsubscribes a member that was released before its ack came back', async () => {
        // A component that mounts and unmounts in one tick, which React does in strict mode, is
        // already in the batch by the time the release lands. Its id has to be given back or the
        // subscription outlives everything holding it.
        const {client, store} = batching();
        const release = store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        release();
        await flush();

        client.batches[0].resolve({batchId: 'batch-1', subscriptionIds: ['sub-monitors', 'sub-results']});
        await flush();

        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-monitors']);
        expect(store.getEntry(results, null, shape).status).toBe('ready');
    });

    it('sends nothing for a subscription already closed when the batch goes out', async () => {
        // reset closes synchronously, so the queue can hold entries with nothing left behind them.
        // Sending those opens subscriptions on the server that no release will ever reach.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.reset();
        await flush();

        expect(client.batches).toHaveLength(0);
        expect(client.subscribes).toHaveLength(0);
    });

    it('opens a batch per tick rather than accumulating across them', async () => {
        // A subscription that starts after the page has settled cannot wait for a batch that will
        // never be filled, so each tick has to close its own.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        await flush();
        store.subscribe(results, null, shape);
        await flush();

        expect(client.batches).toHaveLength(2);
        expect(client.batches[0].members.map(member => member.rql)).toEqual([monitors]);
        expect(client.batches[1].members.map(member => member.rql)).toEqual([results]);
    });

    it('falls back to one subscribe each when the transport has no batch path', async () => {
        // A store is handed whatever client the app was built with. Asking for batching against a
        // transport that cannot do it has to degrade, not fail the page.
        const client = new FakeClient();
        const store = new Store(client, {batch: true});
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();

        expect(client.subscribes.map(call => call.rql)).toEqual([monitors, results]);
    });

    it('still shares one member between two subscribers of the same rql', async () => {
        // Deduplication happens before anything is queued, so batching must not turn one shared
        // subscription into two members that both deliver into the same entry.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(monitors, null, shape);
        await flush();

        expect(client.batches[0].members).toHaveLength(1);
    });

    it('delivers a change to the member that asked for it', async () => {
        // Every member carries its own callbacks into the batch. If they were shared or mixed up the
        // rows would land on another entry, which no id check would catch.
        const {client, store} = batching();
        store.subscribe(monitors, null, shape);
        store.subscribe(results, null, shape);
        await flush();
        client.batches[0].resolve({batchId: 'batch-1', subscriptionIds: ['sub-monitors', 'sub-results']});
        await flush();

        client.batches[0].members[1].callbacks.onInsert?.([{'#rownum': 1, id: 7, name: 'r'}]);

        expect(store.getEntry(monitors, null, shape).data).toEqual([]);
        expect(store.getEntry(results, null, shape).data).toEqual([{id: 7, name: 'r'}]);
    });
});
