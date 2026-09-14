// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {DurationValue, ReifyError, Shape, encodeParams} from '@reifydb/core';
import {WsClient} from '@reifydb/client';
import type {SubscriptionConfig} from '@reifydb/client';
import {Store} from '../src';
import {flush} from './fake-client';
import {ScriptedSocket} from './scripted-socket';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items filter { owner == $owner }';
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

async function batchOfTwo(config?: SubscriptionConfig): Promise<Store> {
    const store = new Store(await connect(), {batch: true});
    store.subscribe(rql, {owner: 'a'}, shape, config);
    store.subscribe(rql, {owner: 'b'}, shape, config);
    await flush();
    return store;
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

describe('batched subscription params', () => {
    it('sends each subscription its own params in the batch', async () => {
        // A batch that drops params runs every subscription unfiltered, so each must carry its own at its own index.
        await batchOfTwo();

        const [batch] = sockets[0].requests('BatchSubscribe');

        expect(batch.payload.subscriptions.map((subscription: any) => subscription.params)).toEqual([encodeParams({owner: 'a'}), encodeParams({owner: 'b'})]);
    });

    it('sends the same params again when the batch is re-established after a reconnect', async () => {
        // The resubscribe must carry the params again, otherwise a reconnect silently widens every filtered subscription.
        await batchOfTwo(tuned);
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();

        const second = await dropConnection();
        const [batch] = second.requests('BatchSubscribe');

        expect(batch.payload.subscriptions).toEqual(sockets[0].requests('BatchSubscribe')[0].payload.subscriptions);
        expect(batch.payload.subscriptions.map((subscription: any) => subscription.params)).toEqual([encodeParams({owner: 'a'}), encodeParams({owner: 'b'})]);
    });
});

describe('a refused resubscribe', () => {
    it('marks the entry errored with the refusal instead of leaving the stale rows ready', async () => {
        // The old rows stop updating once the resubscribe is refused, so the entry must never stay ready.
        const store = new Store(await connect());
        store.subscribe(rql, {owner: 'a'}, shape);
        sockets[0].ackSubscribe('server-1');
        sockets[0].insert('server-1', [[1, 1, 'a']]);
        await flush();

        const second = await dropConnection();
        second.refuse('Subscribe', 'AUTH_REQUIRED');
        await flush();

        const entry = store.getEntry(rql, {owner: 'a'}, shape);
        expect(entry.status).toBe('error');
        expect(entry.error).toBeInstanceOf(ReifyError);
        expect((entry.error as ReifyError).code).toBe('AUTH_REQUIRED');
    });

    it('marks every subscription of a refused batch errored', async () => {
        // The whole batch is refused at once, so every subscription must go to error, not just the first one.
        const store = await batchOfTwo();
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();

        const second = await dropConnection();
        second.refuse('BatchSubscribe', 'AUTH_REQUIRED');
        await flush();

        expect(store.getEntry(rql, {owner: 'a'}, shape).status).toBe('error');
        expect(store.getEntry(rql, {owner: 'b'}, shape).status).toBe('error');
        expect((store.getEntry(rql, {owner: 'b'}, shape).error as ReifyError).code).toBe('AUTH_REQUIRED');
    });

    it('hands the refusal to the subscription onError, the only path left to the caller', async () => {
        // The subscribe promise settled long before the reconnect, so without onError the refusal never reaches the app.
        const ws = await connect();
        const errors: Error[] = [];
        const subscribed = ws.subscribe(rql, {owner: 'a'}, shape, {onError: error => errors.push(error)});
        sockets[0].ackSubscribe('server-1');
        await subscribed;

        const second = await dropConnection();
        second.refuse('Subscribe', 'AUTH_REQUIRED');
        await flush();

        expect(errors).toHaveLength(1);
        expect((errors[0] as ReifyError).code).toBe('AUTH_REQUIRED');
    });
});

describe('a partial resubscribe ack', () => {
    it('marks a subscription the ack left out errored instead of leaving its stale rows ready', async () => {
        // A subscription the server did not re-establish never updates again, so it must not stay ready.
        const store = await batchOfTwo();
        sockets[0].ackBatch('batch-1', ['server-1', 'server-2']);
        await flush();

        const second = await dropConnection();
        second.ackBatch('batch-2', ['server-3', null]);
        await flush();

        expect(store.getEntry(rql, {owner: 'a'}, shape).status).toBe('ready');
        expect(store.getEntry(rql, {owner: 'b'}, shape).status).toBe('error');
    });
});

describe('a throwing onResubscribe', () => {
    it('hands the throw to onError and still re-establishes the subscriptions after it', async () => {
        // A throw must not escape into the socket handler and stall the loop, or every later subscription stays dead.
        const ws = await connect();
        const errors: Error[] = [];
        const thrown = new Error('onResubscribe failed');
        ws.subscribe(rql, {owner: 'a'}, shape, {
            onError: error => errors.push(error),
            onResubscribe: () => {
                throw thrown;
            },
        });
        sockets[0].ackSubscribe('server-1');
        ws.subscribe(rql, {owner: 'b'}, shape, {});
        sockets[0].ackSubscribe('server-2');
        await flush();

        const second = await dropConnection();
        expect(() => second.ackSubscribe('server-3')).not.toThrow();
        await flush();

        expect(errors).toEqual([thrown]);
        expect(second.requests('Subscribe')).toHaveLength(2);
    });
});

describe('unsubscribing an id this client does not hold', () => {
    it('resolves without asking the server, so it can never close another connection\'s subscription', async () => {
        // An id this client never issued may belong to another connection, so it must never reach the server.
        const ws = await connect();

        const unsubscribed = ws.unsubscribe('server-7');
        await flush();

        expect(sockets[0].unsubscribed()).toEqual([]);
        await unsubscribed;
    });
});
