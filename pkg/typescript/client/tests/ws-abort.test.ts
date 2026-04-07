// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {WsClient} from '../src/ws';
import {
    createMockSocket,
    MockSocket,
    setupWindowWebSocket,
    teardownWindowWebSocket,
} from './helpers/abort-test-utils';

describe('WsClient abort signal', () => {
    let mockSocket: MockSocket;

    beforeEach(() => {
        mockSocket = createMockSocket();
        setupWindowWebSocket(mockSocket);
    });

    afterEach(() => {
        teardownWindowWebSocket();
        vi.restoreAllMocks();
    });

    describe('connect with signal', () => {
        it('throws AbortError immediately when signal is pre-aborted', async () => {
            const controller = new AbortController();
            controller.abort();

            await expect(WsClient.connect({url: 'ws://test', signal: controller.signal}))
                .rejects.toThrow('AbortError');

            // WebSocket constructor should never be called
            expect(globalThis.WebSocket).not.toHaveBeenCalled();
        });

        it('throws AbortError when signal aborts during connection wait', async () => {
            const controller = new AbortController();

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            // Let connect() reach the addEventListener registration
            await Promise.resolve();
            controller.abort();

            await expect(connectPromise).rejects.toThrow('AbortError');
            expect(mockSocket.close).toHaveBeenCalled();
        });

        it('connects successfully when signal is provided but not aborted', async () => {
            const controller = new AbortController();

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            // Simulate socket opening
            await Promise.resolve();
            mockSocket._emit('open');

            const client = await connectPromise;
            expect(client).toBeDefined();
            client.disconnect();
        });

        it('removes abort listener from signal after successful connection', async () => {
            const controller = new AbortController();
            const removeEventListenerSpy = vi.spyOn(controller.signal, 'removeEventListener');

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            mockSocket._emit('open');

            const client = await connectPromise;
            expect(removeEventListenerSpy).toHaveBeenCalledWith('abort', expect.any(Function));
            client.disconnect();
        });

        it('throws AbortError on post-connection race condition', async () => {
            const controller = new AbortController();

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            // Fire open event and then immediately abort — the post-await check should catch it
            mockSocket._emit('open');
            controller.abort();

            await expect(connectPromise).rejects.toThrow('AbortError');
            expect(mockSocket.close).toHaveBeenCalled();
        });

        it('cleans up socket event listeners on successful connection', async () => {
            const controller = new AbortController();

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            mockSocket._emit('open');

            const client = await connectPromise;
            // The cleanup function should have removed 'open' and 'error' listeners
            expect(mockSocket.removeEventListener).toHaveBeenCalledWith('open', expect.any(Function));
            expect(mockSocket.removeEventListener).toHaveBeenCalledWith('error', expect.any(Function));
            client.disconnect();
        });

        it('cleans up socket event listeners on abort', async () => {
            const controller = new AbortController();

            const connectPromise = WsClient.connect({url: 'ws://test', signal: controller.signal});

            await Promise.resolve();
            controller.abort();

            await expect(connectPromise).rejects.toThrow('AbortError');
            expect(mockSocket.removeEventListener).toHaveBeenCalledWith('open', expect.any(Function));
            expect(mockSocket.removeEventListener).toHaveBeenCalledWith('error', expect.any(Function));
        });
    });
});
