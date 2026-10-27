// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {Shape} from '@reifydb/core';
import {WsClient} from '../src/ws';
import {encodeParams} from '../src/encoder';
import {rbcf} from '../src/rbcf';
import type {SubscribeItem, SubscriptionCallbacks} from '../src/types';
import {ScriptedSocket} from './helpers/scripted-socket';

const shape = Shape.object({id: Shape.number()});

let sockets: ScriptedSocket[] = [];
let client: WsClient | undefined;

function flush(): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, 0));
}

function item(table: string, callbacks: SubscriptionCallbacks = {}): SubscribeItem {
    return {rql: `from test::${table}`, shape, callbacks};
}

async function connect(): Promise<WsClient> {
    client = await WsClient.connect({url: 'ws://test', reconnectDelayMs: 0});
    return client;
}

async function subscribed(items: SubscribeItem[], ids: string[]): Promise<string[]> {
    const socket = sockets[sockets.length - 1];
    const result = client!.subscribe(items);
    socket.ackSubscribe(ids);
    return result;
}

async function dropConnection(): Promise<ScriptedSocket> {
    sockets[sockets.length - 1].close();
    await flush();
    return sockets[sockets.length - 1];
}

function rqls(request: any): string[] {
    return request.payload.subscriptions.map((subscription: any) => subscription.rql);
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
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(async () => {
    await client?.disconnect();
    client = undefined;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
});

describe('subscribe sends a list', () => {
    it('sends one Subscribe frame for a list of subscriptions', async () => {
        // One frame per item would let a partial failure split one call's all-or-nothing scope.
        await connect();
        const a: SubscribeItem = {...item('a'), params: {x: 1}, config: {hydration: {enabled: true, maxRows: 50}}};
        const b = item('b');

        const result = client!.subscribe([a, b]);

        const requests = sockets[0].requests('Subscribe');
        expect(requests).toHaveLength(1);
        expect(rqls(requests[0])).toEqual(['from test::a', 'from test::b']);
        expect(requests[0].payload.subscriptions[0].params).toEqual(encodeParams({x: 1}));
        expect(requests[0].payload.subscriptions[0].options).toEqual({hydration: {enabled: true, max_rows: 50}});
        expect(requests[0].payload.subscriptions[1].params).toBeUndefined();
        expect(requests[0].payload.subscriptions[1].options).toEqual({hydration: {enabled: true}});

        sockets[0].ackSubscribe(['s1', 's2']);
        await expect(result).resolves.toEqual(['s1', 's2']);
    });

    it('maps acked ids to the caller by index, not by ack order', async () => {
        // Ids taken in ack order would hand each caller the other item's subscription.
        await connect();
        const result = client!.subscribe([item('a'), item('b')]);

        sockets[0].ackSubscribeEntries([{index: 1, subscription_id: 's2'}, {index: 0, subscription_id: 's1'}]);

        await expect(result).resolves.toEqual(['s1', 's2']);
    });

    it('rejects an empty list without sending a frame', async () => {
        // An empty Subscribe frame would leave the caller waiting on an ack that names nothing.
        await connect();

        const result = client!.subscribe([]);

        expect(sockets[0].requests('Subscribe')).toHaveLength(0);
        await expect(result).rejects.toThrow(/at least one subscription/i);
    });
});

describe('unsubscribe sends a list', () => {
    it('sends one Unsubscribe frame for every id in the call', async () => {
        // A frame per id would multiply round trips for every list release.
        await connect();
        await subscribed([item('a'), item('b')], ['s1', 's2']);

        const result = client!.unsubscribe(['s1', 's2']);

        expect(sockets[0].unsubscribed()).toEqual([['s1', 's2']]);
        sockets[0].ackUnsubscribe();
        await expect(result).resolves.toBeUndefined();
    });

    it('never sends an id the client does not hold', async () => {
        // An unheld id reaching the server could release a subscription another client owns.
        await connect();
        await subscribed([item('a')], ['s1']);

        await client!.unsubscribe(['x']);
        expect(sockets[0].requests('Unsubscribe')).toHaveLength(0);

        const result = client!.unsubscribe(['s1', 'x']);
        expect(sockets[0].unsubscribed()).toEqual([['s1']]);
        sockets[0].ackUnsubscribe();
        await result;
    });
});

describe('pushes are routed per subscription', () => {
    it('routes each Change entry to its own subscription', async () => {
        // Delivering every entry to one subscription would mix rows across queries.
        await connect();
        const a = item('a', {onInsert: vi.fn()});
        const b = item('b', {onInsert: vi.fn()});
        await subscribed([a, b], ['s1', 's2']);

        sockets[0].change([{subscriptionId: 's2', ids: [2]}, {subscriptionId: 's1', ids: [1]}]);

        expect(a.callbacks.onInsert).toHaveBeenCalledTimes(1);
        expect(a.callbacks.onInsert).toHaveBeenCalledWith([{id: 1}]);
        expect(b.callbacks.onInsert).toHaveBeenCalledTimes(1);
        expect(b.callbacks.onInsert).toHaveBeenCalledWith([{id: 2}]);
    });

    it('closes only the named subscription and forgets it', async () => {
        // A closed subscription left in the registry would keep receiving changes and send a stale release.
        await connect();
        const a = item('a', {onInsert: vi.fn(), onClosed: vi.fn()});
        const b = item('b', {onInsert: vi.fn(), onClosed: vi.fn()});
        await subscribed([a, b], ['s1', 's2']);

        sockets[0].closed('s1');

        expect(a.callbacks.onClosed).toHaveBeenCalledTimes(1);
        expect(b.callbacks.onClosed).not.toHaveBeenCalled();

        sockets[0].change([{subscriptionId: 's1', ids: [1]}]);
        expect(a.callbacks.onInsert).not.toHaveBeenCalled();

        await client!.unsubscribe(['s1']);
        expect(sockets[0].requests('Unsubscribe')).toHaveLength(0);
    });

    it('contains a throwing onClosed so later messages still arrive', async () => {
        // A throw escaping the socket handler would stall every later message on the connection.
        await connect();
        const boom = new Error('boom');
        const a = item('a', {onClosed: () => { throw boom; }, onError: vi.fn()});
        const b = item('b', {onInsert: vi.fn()});
        await subscribed([a, b], ['s1', 's2']);

        sockets[0].closed('s1');
        sockets[0].change([{subscriptionId: 's2', ids: [2]}]);

        expect(a.callbacks.onError).toHaveBeenCalledTimes(1);
        expect(a.callbacks.onError).toHaveBeenCalledWith(boom);
        expect(b.callbacks.onInsert).toHaveBeenCalledWith([{id: 2}]);
    });

    it('reports an undecodable binary entry to its own subscription only', async () => {
        // One bad entry must not drop the entries after it nor vanish into the console.
        await connect();
        const a = item('a', {onInsert: vi.fn(), onError: vi.fn()});
        const b = item('b', {onInsert: vi.fn(), onError: vi.fn()});
        await subscribed([a, b], ['s1', 's2']);
        const valid = rbcf.encode([{op: 1, columns: [{name: 'id', type: 'Int4', payload: ['7']}]}]);
        const garbage = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

        sockets[0].changeList([['s2', garbage], ['s1', valid]]);

        expect(b.callbacks.onError).toHaveBeenCalledTimes(1);
        expect(b.callbacks.onInsert).not.toHaveBeenCalled();
        expect(a.callbacks.onInsert).toHaveBeenCalledTimes(1);
        expect(a.callbacks.onInsert).toHaveBeenCalledWith([{id: 7}]);
        expect(a.callbacks.onError).not.toHaveBeenCalled();
    });
});

describe('reconnect resubscribes per original call', () => {
    it('resubscribes each call as its own frame, oldest first, one at a time', async () => {
        // Merging calls widens one call's failure to all; splitting them multiplies frames.
        await connect();
        await subscribed([item('a'), item('b')], ['s1', 's2']);
        await subscribed([item('c')], ['s3']);

        const second = await dropConnection();

        expect(second.requests('Subscribe')).toHaveLength(1);
        expect(rqls(second.requests('Subscribe')[0])).toEqual(['from test::a', 'from test::b']);

        second.ackSubscribe(['n1', 'n2']);
        await flush();

        expect(second.requests('Subscribe')).toHaveLength(2);
        expect(rqls(second.requests('Subscribe')[1])).toEqual(['from test::c']);
        second.ackSubscribe(['n3']);
    });

    it('leaves a released member out of its call on reconnect', async () => {
        // A released member resubscribed on reconnect would leak a live server subscription.
        await connect();
        await subscribed([item('a'), item('b')], ['s1', 's2']);
        const released = client!.unsubscribe(['s1']);
        sockets[0].ackUnsubscribe();
        await released;

        const second = await dropConnection();

        expect(second.requests('Subscribe')).toHaveLength(1);
        expect(rqls(second.requests('Subscribe')[0])).toEqual(['from test::b']);
        second.ackSubscribe(['n2']);
    });

    it('gives back members released during the resubscribe in one frame', async () => {
        // A release per member multiplies frames, and onResubscribe must not fire for a released member.
        await connect();
        const a = item('a', {onResubscribe: vi.fn()});
        const b = item('b', {onResubscribe: vi.fn()});
        await subscribed([a, b], ['s1', 's2']);

        const second = await dropConnection();
        await client!.unsubscribe(['s1', 's2']);
        expect(second.requests('Unsubscribe')).toHaveLength(0);

        second.ackSubscribe(['n1', 'n2']);
        await flush();

        expect(second.unsubscribed()).toEqual([['n1', 'n2']]);
        expect(a.callbacks.onResubscribe).not.toHaveBeenCalled();
        expect(b.callbacks.onResubscribe).not.toHaveBeenCalled();
    });
});
