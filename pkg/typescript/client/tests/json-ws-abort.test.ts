// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {JsonWsClient} from '../src/json-ws';
import {
    create_mock_socket,
    MockSocket,
    setup_window_web_socket,
    teardown_window_web_socket,
} from './helpers/abort-test-utils';

describe('JsonWsClient abort signal', () => {
    let mock_socket: MockSocket;

    beforeEach(() => {
        mock_socket = create_mock_socket();
        setup_window_web_socket(mock_socket);
    });

    afterEach(() => {
        teardown_window_web_socket();
        vi.restoreAllMocks();
    });

    describe('connect with signal', () => {
        it('throws AbortError immediately when signal is pre-aborted', async () => {
            const controller = new AbortController();
            controller.abort();

            await expect(JsonWsClient.connect({url: 'ws://test', signal: controller.signal}))
                .rejects.toThrow('AbortError');

            // WebSocket constructor should never be called
            expect(globalThis.WebSocket).not.toHaveBeenCalled();
        });

        it('throws AbortError when signal aborts during connection wait', async () => {
            const controller = new AbortController();

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            // Let connect() reach the addEventListener registration
            await Promise.resolve();
            controller.abort();

            await expect(connect_promise).rejects.toThrow('AbortError');
            expect(mock_socket.close).toHaveBeenCalled();
        });

        it('connects successfully when signal is provided but not aborted', async () => {
            const controller = new AbortController();

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            // Simulate socket opening
            await Promise.resolve();
            mock_socket._emit('open');

            const client = await connect_promise;
            expect(client).toBeDefined();
            client.disconnect();
        });

        it('removes abort listener from signal after successful connection', async () => {
            const controller = new AbortController();
            const removeEventListenerSpy = vi.spyOn(controller.signal, 'removeEventListener');

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            mock_socket._emit('open');

            const client = await connect_promise;
            expect(removeEventListenerSpy).toHaveBeenCalledWith('abort', expect.any(Function));
            client.disconnect();
        });

        it('throws AbortError on post-connection race condition', async () => {
            const controller = new AbortController();

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            // Fire open event and then immediately abort — the post-await check should catch it
            mock_socket._emit('open');
            controller.abort();

            await expect(connect_promise).rejects.toThrow('AbortError');
            expect(mock_socket.close).toHaveBeenCalled();
        });

        it('cleans up socket event listeners on successful connection', async () => {
            const controller = new AbortController();

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            mock_socket._emit('open');

            const client = await connect_promise;
            expect(mock_socket.removeEventListener).toHaveBeenCalledWith('open', expect.any(Function));
            expect(mock_socket.removeEventListener).toHaveBeenCalledWith('error', expect.any(Function));
            client.disconnect();
        });

        it('cleans up socket event listeners on abort', async () => {
            const controller = new AbortController();

            const connect_promise = JsonWsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            controller.abort();

            await expect(connect_promise).rejects.toThrow('AbortError');
            expect(mock_socket.removeEventListener).toHaveBeenCalledWith('open', expect.any(Function));
            expect(mock_socket.removeEventListener).toHaveBeenCalledWith('error', expect.any(Function));
        });
    });
});
