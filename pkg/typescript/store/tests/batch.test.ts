// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store, rql} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const monitors = rql(shape)`from test::monitors`;
const results = rql(shape)`from test::results`;
const regions = rql(shape)`from test::regions`;

function batching() {
    const client = new FakeClient();
    const errors: Error[] = [];
    const store = new Store(client, {onBackgroundError: error => errors.push(error)});
    return {client, store, errors};
}

describe('batched subscription opening', () => {
    it('opens every subscription started in one tick as a single batch', async () => {
        // React runs a page's mount effects in one synchronous pass, so this is the whole point:
        // six hooks on a dashboard become one round trip rather than six.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(results, null);
        store.subscribe(regions, null);
        await flush();

        expect(client.subscribes).toHaveLength(1);
        expect(client.subscribes[0].subscriptions.map(subscription => subscription.rql)).toEqual([monitors.rql, results.rql, regions.rql]);
    });

    it('turns each entry ready with the id the ack gave that subscription', async () => {
        // The ack reports ids positionally against the subscriptions that were sent. Reading them in the
        // wrong order would leave every entry ready and every later unsubscribe aimed elsewhere.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(results, null);
        await flush();

        client.subscribes[0].resolve(['sub-monitors', 'sub-results']);
        await flush();

        expect(store.getEntry(monitors, null).status).toBe('ready');
        expect(store.getEntry(results, null).status).toBe('ready');

        store.reset();
        await flush();
        expect(client.unsubscribes).toHaveLength(1);
        expect([...client.unsubscribes[0].subscriptionIds].sort()).toEqual([
            'sub-monitors',
            'sub-results'
        ]);
    });

    it('marks every subscription in error when the batch itself is refused', async () => {
        // One ack covers all of them, so a refusal is not one subscription's problem. An entry left
        // loading would render as a spinner that never resolves.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(results, null);
        await flush();

        const error = new Error('denied');
        client.subscribes[0].reject(error);
        await flush();

        expect(store.getEntry(monitors, null).status).toBe('error');
        expect(store.getEntry(monitors, null).error).toBe(error);
        expect(store.getEntry(results, null).status).toBe('error');
    });

    it('marks a subscription in error when the ack names no id for it', async () => {
        // A short ack is the one failure that looks like success. Without this the entry would stay
        // loading for the life of the page with nothing recorded anywhere.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(results, null);
        await flush();

        client.subscribes[0].resolve(['sub-monitors']);
        await flush();

        expect(store.getEntry(monitors, null).status).toBe('ready');
        expect(store.getEntry(results, null).status).toBe('error');
    });

    it('unsubscribes a subscription that was released before its ack came back', async () => {
        // A component that mounts and unmounts in one tick, which React does in strict mode, is
        // already in the batch by the time the release lands. Its id has to be given back or the
        // subscription outlives everything holding it.
        const {client, store} = batching();
        const release = store.subscribe(monitors, null);
        store.subscribe(results, null);
        release();
        await flush();

        client.subscribes[0].resolve(['sub-monitors', 'sub-results']);
        await flush();

        expect(client.unsubscribes.map(call => call.subscriptionIds)).toEqual([['sub-monitors']]);
        expect(store.getEntry(results, null).status).toBe('ready');
    });

    it('sends nothing for a subscription already closed when the batch goes out', async () => {
        // reset closes synchronously, so the queue can hold entries with nothing left behind them.
        // Sending those opens subscriptions on the server that no release will ever reach.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.reset();
        await flush();

        expect(client.subscribes).toHaveLength(0);
    });

    it('opens a batch per tick rather than accumulating across them', async () => {
        // A subscription that starts after the page has settled cannot wait for a batch that will
        // never be filled, so each tick has to close its own.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        await flush();
        store.subscribe(results, null);
        await flush();

        expect(client.subscribes).toHaveLength(2);
        expect(client.subscribes[0].subscriptions.map(subscription => subscription.rql)).toEqual([monitors.rql]);
        expect(client.subscribes[1].subscriptions.map(subscription => subscription.rql)).toEqual([results.rql]);
    });

    it('still shares one subscription between two subscribers of the same rql', async () => {
        // Deduplication happens before anything is queued, so batching must not turn one shared
        // subscription into two subscriptions that both deliver into the same entry.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(monitors, null);
        await flush();

        expect(client.subscribes).toHaveLength(1);
        expect(client.subscribes[0].subscriptions).toHaveLength(1);
    });

    it('delivers a change to the subscription that asked for it', async () => {
        // Every subscription carries its own callbacks into the batch. If they were shared or mixed up the
        // rows would land on another entry, which no id check would catch.
        const {client, store} = batching();
        store.subscribe(monitors, null);
        store.subscribe(results, null);
        await flush();
        client.subscribes[0].resolve(['sub-monitors', 'sub-results']);
        await flush();

        client.subscribes[0].subscriptions[1].callbacks.onInsert?.([{'#rownum': 1, id: 7, name: 'r'}]);

        expect(store.getEntry(monitors, null).data).toEqual([]);
        expect(store.getEntry(results, null).data).toEqual([{id: 7, name: 'r'}]);
    });

    it('gives back every subscription released in one tick in a single unsubscribe', async () => {
        // An unsubscribe per release would turn a page unmount into one round trip per hook.
        const {client, store} = batching();
        const releaseMonitors = store.subscribe(monitors, null);
        const releaseResults = store.subscribe(results, null);
        await flush();
        client.subscribes[0].resolve(['sub-monitors', 'sub-results']);
        await flush();

        releaseMonitors();
        releaseResults();
        await flush();

        expect(client.unsubscribes.map(call => call.subscriptionIds)).toEqual([['sub-monitors', 'sub-results']]);
    });

    it('gives back every subscription released before the ack in a single unsubscribe', async () => {
        // Each late id given back on its own would cost one unsubscribe per hook of an unmounted page.
        const {client, store} = batching();
        const releaseMonitors = store.subscribe(monitors, null);
        const releaseResults = store.subscribe(results, null);
        await flush();
        releaseMonitors();
        releaseResults();
        await flush();
        expect(client.unsubscribes).toHaveLength(0);

        client.subscribes[0].resolve(['sub-monitors', 'sub-results']);
        await flush();

        expect(client.unsubscribes.map(call => call.subscriptionIds)).toEqual([['sub-monitors', 'sub-results']]);
    });

    it('sends the unsubscribe of a tick before its subscribe', async () => {
        // Subscribing first would hold both the old and the new server subscription during a params change.
        const {client, store} = batching();
        const order: string[] = [];
        const subscribe = client.subscribe.bind(client);
        const unsubscribe = client.unsubscribe.bind(client);
        client.subscribe = subscriptions => {
            order.push('subscribe');
            return subscribe(subscriptions);
        };
        client.unsubscribe = subscriptionIds => {
            order.push('unsubscribe');
            return unsubscribe(subscriptionIds);
        };
        const release = store.subscribe(monitors, null);
        await flush();
        client.subscribes[0].resolve(['sub-monitors']);
        await flush();
        order.length = 0;

        release();
        store.subscribe(results, null);
        await flush();

        expect(order).toEqual(['unsubscribe', 'subscribe']);
        expect(client.unsubscribes.map(call => call.subscriptionIds)).toEqual([['sub-monitors']]);
        expect(client.subscribes[1].subscriptions.map(subscription => subscription.rql)).toEqual([results.rql]);
    });
});
