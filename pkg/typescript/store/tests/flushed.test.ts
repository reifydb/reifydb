// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store, rql} from '../src';
import {FakeClient, flush} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const items = rql(shape)`from test::items`;
const others = rql(shape)`from test::others`;

function setup() {
    const client = new FakeClient();
    const store = new Store(client);
    return {client, store};
}

function track(promise: Promise<void>): {settled: () => boolean} {
    let settled = false;
    promise.then(() => {
        settled = true;
    });
    return {settled: () => settled};
}

describe('flushed', () => {
    it('resolves at once on an idle store', async () => {
        // A store with nothing queued must not make a caught-up wait hang forever.
        const {store} = setup();
        const wait = track(store.flushed());
        await flush();
        expect(wait.settled()).toBe(true);
    });

    it('waits for a subscribe whose flush has not run yet, and the entry is ready when it resolves', async () => {
        // Called in the same tick as subscribe, before the batch is sent; resolving here is the race that made the probes test read an empty page.
        const {client, store} = setup();
        store.subscribe(items, null);
        const done = store.flushed();
        const wait = track(done);
        await flush();
        expect(client.subscribes).toHaveLength(1);
        expect(wait.settled()).toBe(false);

        client.subscribes[0].resolve(['sub-1']);
        await done;
        expect(store.getEntry(items, null).status).toBe('ready');
    });

    it('stays pending while the ack is held open', async () => {
        // Sending the batch is not enough; until the ack is handled the entry is still loading.
        const {client, store} = setup();
        store.subscribe(items, null);
        await flush();
        const wait = track(store.flushed());
        await flush();
        await flush();
        expect(wait.settled()).toBe(false);
        expect(store.getEntry(items, null).status).toBe('loading');

        client.subscribes[0].resolve(['sub-1']);
        await flush();
        expect(wait.settled()).toBe(true);
    });

    it('resolves on a rejected subscribe, with the entry already in error', async () => {
        // A refusal belongs on the entry; rejecting here would fail every wait on a page with one refused query.
        const {client, store} = setup();
        store.subscribe(items, null);
        const done = store.flushed();
        const wait = track(done);
        await flush();
        expect(wait.settled()).toBe(false);
        expect(store.getEntry(items, null).status).toBe('loading');

        client.subscribes[0].reject(new Error('refused'));
        await expect(done).resolves.toBeUndefined();
        expect(store.getEntry(items, null).status).toBe('error');
        expect(store.getEntry(items, null).error?.message).toBe('refused');
    });

    it('waits for a batch started while the first ack is in flight', async () => {
        // A second component mounting mid-wait queues a new batch; covering only the first would let its entry be read while loading.
        const {client, store} = setup();
        store.subscribe(items, null);
        const done = store.flushed();
        await flush();
        store.subscribe(others, null);
        await flush();
        expect(client.subscribes).toHaveLength(2);

        client.subscribes[0].resolve(['sub-1']);
        await flush();
        const wait = track(done);
        await flush();
        expect(wait.settled()).toBe(false);

        client.subscribes[1].resolve(['sub-2']);
        await done;
        expect(store.getEntry(others, null).status).toBe('ready');
    });

    it('does not wait for an unsubscribe to be acked', async () => {
        // Only subscribe acks are tracked; waiting on releases would hang a test whose unsubscribe never settles.
        const {client, store} = setup();
        const release = store.subscribe(items, null);
        await flush();
        client.subscribes[0].resolve(['sub-1']);
        await flush();

        release();
        await flush();
        expect(client.unsubscribes).toHaveLength(1);
        const wait = track(store.flushed());
        await flush();
        expect(wait.settled()).toBe(true);
    });
});
