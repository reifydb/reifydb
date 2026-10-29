// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {Shape} from '@reifydb/core';
import {WsClient} from '../src/ws';
import {JsonWsClient} from '../src/json-ws';
import {ReifyError} from '../src/types';
import {ScriptedSocket} from './helpers/scripted-socket';

let sockets: ScriptedSocket[] = [];

function flush(): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, 0));
}

function last(): ScriptedSocket {
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
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
});

describe('ws session resume', () => {
    it('rejects connect with the server error when the token is refused, and closes without retrying', async () => {
        // A dead token used to resolve and then fail every request silently; the caller must see why, once.
        const connecting = WsClient.connect({url: 'ws://test', token: 'dead', reconnectDelayMs: 0});
        await flush();
        last().failAuth('AUTH_FAILED');

        const error = await connecting.catch((e: unknown) => e);
        expect(error).toBeInstanceOf(ReifyError);
        expect((error as ReifyError).code).toBe('AUTH_FAILED');
        await flush();
        expect(sockets).toHaveLength(1);
        expect(sockets[0].readyState).toBe(3);
    });

    it('resolves connect only after the server accepts the token', async () => {
        // A request sent before the reply would run with no identity and fail AUTH_REQUIRED.
        let resolved = false;
        const connecting = WsClient.connect({url: 'ws://test', token: 'live'}).then(client => {
            resolved = true;
            return client;
        });
        await flush();
        expect(resolved).toBe(false);
        expect(last().requests('Auth')).toEqual([expect.objectContaining({type: 'Auth', payload: {token: 'live'}})]);

        last().ackAuth();
        const client = await connecting;
        expect(resolved).toBe(true);
        await client.disconnect();
    });

    it('connects without a token at once and sends no auth request', async () => {
        // Anonymous watchers must not wait on a round trip they do not need.
        const client = await WsClient.connect({url: 'ws://test'});
        expect(last().requests('Auth')).toEqual([]);
        await client.disconnect();
    });

    it('closes and reports onAuthFailed when a reconnect is refused, without resubscribing', async () => {
        // After a server restart the token is dead; resubscribing would only add AUTH_REQUIRED noise.
        const onAuthFailed = vi.fn();
        const onReconnect = vi.fn();
        const connecting = WsClient.connect({url: 'ws://test', token: 'live', reconnectDelayMs: 0, onAuthFailed, onReconnect});
        await flush();
        last().ackAuth();
        const client = await connecting;
        const subscribing = client.subscribe([{rql: 'from test::t', shape: Shape.object({id: Shape.number()}), callbacks: {}}]);
        last().ackSubscribe(['s1']);
        await subscribing;

        sockets[0].close();
        await flush();
        await flush();
        last().failAuth('AUTH_FAILED');
        await flush();
        await flush();

        expect(onAuthFailed).toHaveBeenCalledTimes(1);
        expect(onAuthFailed.mock.calls[0][0]).toBeInstanceOf(ReifyError);
        expect(onAuthFailed.mock.calls[0][0].code).toBe('AUTH_FAILED');
        expect(onReconnect).not.toHaveBeenCalled();
        expect(last().requests('Subscribe')).toEqual([]);
        expect(last().readyState).toBe(3);
        await flush();
        expect(sockets).toHaveLength(2);
    });

    it('treats a reconnect auth timeout as a network drop and retries instead of ending the session', async () => {
        // Only a real refusal may log the user out; a slow server must not.
        const onAuthFailed = vi.fn();
        const connecting = WsClient.connect({url: 'ws://test', token: 'live', reconnectDelayMs: 0, timeoutMs: 5, onAuthFailed});
        await flush();
        last().ackAuth();
        const client = await connecting;

        sockets[0].close();
        await flush();
        await flush();
        await new Promise(resolve => setTimeout(resolve, 20));
        await flush();

        expect(onAuthFailed).not.toHaveBeenCalled();
        expect(sockets[1].readyState).toBe(3);
        expect(sockets.length).toBeGreaterThanOrEqual(3);
        await client.disconnect();
    });
});

describe('json ws session resume', () => {
    it('rejects connect with the server error when the token is refused', async () => {
        // The json transport must fail the same way, or callers see different behaviour per format.
        const connecting = JsonWsClient.connect({url: 'ws://test', token: 'dead', reconnectDelayMs: 0});
        await flush();
        last().failAuth('AUTH_FAILED');

        const error = await connecting.catch((e: unknown) => e);
        expect(error).toBeInstanceOf(ReifyError);
        expect((error as ReifyError).code).toBe('AUTH_FAILED');
        await flush();
        expect(sockets).toHaveLength(1);
    });

    it('reports onAuthFailed when a reconnect is refused', async () => {
        // Same contract as the binary transport after a server restart.
        const onAuthFailed = vi.fn();
        const connecting = JsonWsClient.connect({url: 'ws://test', token: 'live', reconnectDelayMs: 0, onAuthFailed});
        await flush();
        last().ackAuth();
        await connecting;

        sockets[0].close();
        await flush();
        await flush();
        last().failAuth('AUTH_FAILED');
        await flush();

        expect(onAuthFailed).toHaveBeenCalledTimes(1);
        expect(onAuthFailed.mock.calls[0][0].code).toBe('AUTH_FAILED');
        await flush();
        expect(sockets).toHaveLength(2);
    });
});
