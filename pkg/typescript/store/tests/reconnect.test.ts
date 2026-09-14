// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {DurationValue, Shape} from '@reifydb/core';
import {WsClient} from '@reifydb/client';
import type {SubscriptionConfig} from '@reifydb/client';
import {Store} from '../src';
import {flush} from './fake-client';
import {ScriptedSocket} from './scripted-socket';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items';
const other = 'from test::others';
const tuned: SubscriptionConfig = {hydration: {enabled: true, maxRows: 50}, throttle: DurationValue.fromMilliseconds(250), linger: DurationValue.fromMilliseconds(100)};

let sockets: ScriptedSocket[] = [];
let client: WsClient | undefined;

async function connect(): Promise<WsClient> {
    client = await WsClient.connect({url: 'ws://test', reconnectDelayMs: 0});
    return client;
}

async function dropConnection(): Promise<ScriptedSocket> {
    sockets[sockets.length - 1].close();
    await flush();
    return sockets[sockets.length - 1];
}

beforeEach(() => {
    sockets = [];
    function WebSocket(): ScriptedSocket {
        const socket = new ScriptedSocket();
        sockets.push(socket);
        return socket;
    }
    vi.stubGlobal('WebSocket', WebSocket);
    vi.stubGlobal('window', {WebSocket});
});

afterEach(async () => {
    await client?.disconnect();
    client = undefined;
    vi.unstubAllGlobals();
});

describe('reconnect', () => {
    it('replaces the rows with the snapshot the server re-hydrates after a reconnect', async () => {
        // The snapshot must replace the rows, otherwise a row deleted while offline renders forever.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        sockets[0].insert('server-1', [[1, 1, 'a'], [2, 2, 'b']]);
        await flush();

        const second = await dropConnection();
        second.ackSubscribe('server-2');
        second.insert('server-2', [[1, 1, 'a']]);
        await flush();

        expect(store.getEntry(rql, null, shape).data).toEqual([{id: 1, name: 'a'}]);
    });

    it('empties an entry whose rows were all deleted while disconnected', async () => {
        // An empty snapshot sends no change at all, so the rows must be dropped when the new id is acked.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        sockets[0].insert('server-1', [[1, 1, 'a'], [2, 2, 'b']]);
        await flush();

        const second = await dropConnection();
        second.ackSubscribe('server-2');
        await flush();

        expect(store.getEntry(rql, null, shape).data).toEqual([]);
    });

    it('releases through the id the server issued on reconnect, not the old one', async () => {
        // The old id is dead on the new connection, so a release through it must never happen or the live one leaks.
        const store = new Store(await connect());
        const release = store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        await flush();

        const second = await dropConnection();
        second.ackSubscribe('server-2');
        await flush();
        release();
        await flush();

        expect(second.unsubscribed()).toEqual(['server-2']);
    });

    it('replaces the rows and adopts the new id for a batched subscription after a reconnect', async () => {
        // Pages open through batches, so the batched path must replace rows and adopt ids exactly like a single one.
        const store = new Store(await connect(), {batch: true});
        const release = store.subscribe(rql, null, shape);
        await flush();
        sockets[0].ackBatch('batch-1', ['server-1']);
        sockets[0].insert('server-1', [[1, 1, 'a'], [2, 2, 'b']]);
        await flush();

        const second = await dropConnection();
        second.ackBatch('batch-2', ['server-2']);
        second.insert('server-2', [[1, 1, 'a']]);
        await flush();

        expect(store.getEntry(rql, null, shape).data).toEqual([{id: 1, name: 'a'}]);
        release();
        await flush();
        expect(second.unsubscribed()).toEqual(['server-2']);
    });
});

describe('released batch members', () => {
    it('re-establishes only the members still held after a reconnect', async () => {
        // A released member must never come back on reconnect, or it streams rows nobody reads forever.
        const store = new Store(await connect(), {batch: true});
        const release = store.subscribe(rql, null, shape);
        store.subscribe(other, null, shape, tuned);
        await flush();
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();
        release();
        await flush();
        sockets[0].ackUnsubscribe('server-1');
        await flush();

        const second = await dropConnection();

        const [first] = sockets[0].requests('BatchSubscribe');
        expect(second.requests('BatchSubscribe').map(request => request.payload.subscriptions)).toEqual([[first.payload.subscriptions[1]]]);
    });

    it('sends no batch at all once every member was released', async () => {
        // An emptied batch must be dropped, otherwise every reconnect re-opens subscriptions nobody holds.
        const store = new Store(await connect(), {batch: true});
        const releaseItems = store.subscribe(rql, null, shape);
        const releaseOthers = store.subscribe(other, null, shape);
        await flush();
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();
        releaseItems();
        releaseOthers();
        await flush();
        sockets[0].ackUnsubscribe('server-1');
        sockets[0].ackUnsubscribe('server-2');
        await flush();

        const second = await dropConnection();

        expect(second.requests('BatchSubscribe')).toEqual([]);
    });
});

describe('a second drop during resubscribe', () => {
    it('re-establishes every subscription on the next reconnect instead of losing them', async () => {
        // A drop mid-resubscribe must leave the unfinished ones for the next reconnect, or they go stale forever.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        store.subscribe(other, null, shape);
        sockets[0].ackSubscribe('server-2');
        sockets[0].insert('server-1', [[1, 1, 'a']]);
        sockets[0].insert('server-2', [[1, 1, 'b']]);
        await flush();

        const second = await dropConnection();
        const third = await dropConnection();

        expect(second.requests('Subscribe')).toHaveLength(1);
        expect(third.requests('Subscribe')).toHaveLength(1);
        third.ackSubscribe('server-3');
        await flush();
        third.ackSubscribe('server-4');
        third.insert('server-3', [[2, 2, 'c']]);
        third.insert('server-4', [[2, 2, 'd']]);
        await flush();

        expect(store.getEntry(rql, null, shape)).toMatchObject({status: 'ready', data: [{id: 2, name: 'c'}]});
        expect(store.getEntry(other, null, shape)).toMatchObject({status: 'ready', data: [{id: 2, name: 'd'}]});
    });

    it('re-establishes a subscription acknowledged on the connection that dropped again', async () => {
        // An id acked on the dead connection is dead too, so it must be resubscribed rather than kept as live.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        store.subscribe(other, null, shape);
        sockets[0].ackSubscribe('server-2');
        await flush();

        const second = await dropConnection();
        second.ackSubscribe('server-3');
        await flush();
        const third = await dropConnection();
        third.ackSubscribe('server-4');
        await flush();
        third.ackSubscribe('server-5');
        await flush();

        expect(third.requests('Subscribe')).toHaveLength(2);
        expect(store.getEntry(rql, null, shape).status).toBe('ready');
        expect(store.getEntry(other, null, shape).status).toBe('ready');
    });
});

describe('subscribing while offline', () => {
    it('sends a subscribe started while disconnected once the connection is back', async () => {
        // A subscribe written into the dead socket is lost, so the entry would stay loading forever.
        const store = new Store(await connect());
        sockets[0].close();
        store.subscribe(rql, null, shape);
        await flush();

        expect(sockets[1].requests('Subscribe')).toHaveLength(1);
        sockets[1].ackSubscribe('server-1');
        sockets[1].insert('server-1', [[1, 1, 'a']]);
        await flush();

        expect(store.getEntry(rql, null, shape)).toMatchObject({status: 'ready', data: [{id: 1, name: 'a'}]});
    });

    it('sends a batch started while disconnected once the connection is back', async () => {
        // Pages open through batches, so an offline batch must be held exactly like a single subscribe.
        const store = new Store(await connect(), {batch: true});
        sockets[0].close();
        store.subscribe(rql, null, shape);
        await flush();

        expect(sockets[1].requests('BatchSubscribe')).toHaveLength(1);
        sockets[1].ackBatch('batch-1', ['server-1']);
        await flush();

        expect(store.getEntry(rql, null, shape).status).toBe('ready');
    });

    it('retries a subscribe the dropped connection never acknowledged', async () => {
        // A drop before the ack must not fail the subscribe for good, or the entry errors on a routine blip.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);

        const second = await dropConnection();

        expect(second.requests('Subscribe')).toHaveLength(1);
        second.ackSubscribe('server-1');
        await flush();
        expect(store.getEntry(rql, null, shape).status).toBe('ready');
    });
});

describe('unsubscribing while offline', () => {
    it('resolves an unsubscribe made while disconnected instead of waiting on the dead socket', async () => {
        // The server subscription died with the connection, so waiting on the dead socket hangs the caller forever.
        const ws = await connect();
        const subscribed = ws.subscribe(rql, null, shape, {});
        sockets[0].ackSubscribe('server-1');
        const id = await subscribed;

        sockets[0].close();
        let settled = false;
        ws.unsubscribe(id).then(() => settled = true);
        await flush();

        expect(settled).toBe(true);
    });

    it('never re-establishes a subscription released while disconnected', async () => {
        // A release that only reaches the dead socket must still stop the reconnect from reviving the subscription.
        const store = new Store(await connect());
        const release = store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        await flush();

        sockets[0].close();
        release();
        await flush();

        expect(sockets[1].requests('Subscribe')).toEqual([]);
    });

    it('never re-establishes a batch member released while disconnected', async () => {
        // Pages release batch members one by one, so a member released offline must not come back with its batch.
        const store = new Store(await connect(), {batch: true});
        const release = store.subscribe(rql, null, shape);
        store.subscribe(other, null, shape, tuned);
        await flush();
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();

        sockets[0].close();
        release();
        await flush();

        const [first] = sockets[0].requests('BatchSubscribe');
        expect(sockets[1].requests('BatchSubscribe').map(request => request.payload.subscriptions)).toEqual([[first.payload.subscriptions[1]]]);
    });

    it('never re-establishes a subscription released while a second drop left it waiting', async () => {
        // A subscription waiting to be re-established is still held, so a release must cancel the wait too.
        const store = new Store(await connect());
        const release = store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        await flush();

        await dropConnection();
        sockets[1].close();
        release();
        await flush();

        expect(sockets[2].requests('Subscribe')).toEqual([]);
    });

    it('never sends a resubscribe for a subscription released while the resubscribe was underway', async () => {
        // Resubscribes go out one at a time, so one released before its turn must be skipped rather than revived.
        const store = new Store(await connect());
        store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        const release = store.subscribe(other, null, shape);
        sockets[0].ackSubscribe('server-2');
        await flush();

        const second = await dropConnection();
        release();
        await flush();
        second.ackSubscribe('server-3');
        await flush();

        expect(second.requests('Subscribe')).toHaveLength(1);
    });

    it('gives back the new id of a subscription released while its resubscribe was in flight', async () => {
        // The caller already let go, so the fresh id must be returned to the server, never handed to its callback.
        const ws = await connect();
        const onResubscribe = vi.fn();
        const subscribed = ws.subscribe(rql, null, shape, {onResubscribe});
        sockets[0].ackSubscribe('server-1');
        const id = await subscribed;

        const second = await dropConnection();
        await ws.unsubscribe(id);
        second.ackSubscribe('server-2');
        await flush();

        expect(onResubscribe).not.toHaveBeenCalled();
        expect(second.unsubscribed()).toEqual(['server-2']);
    });

    it('forgets a subscription whose unsubscribe the drop cut off', async () => {
        // The drop ended the server subscription too, so the cut-off unsubscribe must neither fail nor revive it.
        const onBackgroundError = vi.fn();
        const store = new Store(await connect(), {onBackgroundError});
        const release = store.subscribe(rql, null, shape);
        sockets[0].ackSubscribe('server-1');
        await flush();
        release();
        await flush();

        const second = await dropConnection();

        expect(second.requests('Subscribe')).toEqual([]);
        expect(onBackgroundError).not.toHaveBeenCalled();
    });

    it('resolves a batch unsubscribe made while disconnected and never re-establishes the batch', async () => {
        // The batch died with the connection, so waiting on the dead socket hangs and the reconnect revives it.
        const ws = await connect();
        const subscribed = ws.batchSubscribe([{rql, shape, callbacks: {}}]);
        sockets[0].ackBatch('batch-1', ['server-1']);
        const {batchId} = await subscribed;

        sockets[0].close();
        let settled = false;
        ws.batchUnsubscribe(batchId).then(() => settled = true);
        await flush();

        expect(settled).toBe(true);
        expect(sockets[1].requests('BatchSubscribe')).toEqual([]);
    });

    it('never re-establishes a batch unsubscribed while a second drop left its resubscribe waiting', async () => {
        // A batch waiting to be re-established is still held, so unsubscribing it must cancel the wait too.
        const ws = await connect();
        const subscribed = ws.batchSubscribe([{rql, shape, callbacks: {}}]);
        sockets[0].ackBatch('batch-1', ['server-1']);
        const {batchId} = await subscribed;

        await dropConnection();
        sockets[1].close();
        ws.batchUnsubscribe(batchId).catch(error => expect.unreachable(String(error)));
        await flush();

        expect(sockets[2].requests('BatchSubscribe')).toEqual([]);
    });
});
