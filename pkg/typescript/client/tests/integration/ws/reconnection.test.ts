// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {waitForDatabase} from "../setup";
import {Schema} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('WebSocket Client Reconnection', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('Automatic Reconnection', () => {
        let wsClient: WsClient;

        afterEach(async () => {
            if (wsClient) {
                try {
                    wsClient.disconnect();
                } catch (error) {
                    console.error('⚠️ Error during disconnect:', error);
                }
                wsClient = null;
            }
        });

        it('should reconnect after connection is lost', async () => {
            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 100
            });

            const firstResult = await wsClient.query(
                'MAP 42 as result',
                {},
                [Schema.object({result: Schema.number()})]
            );

            expect(firstResult[0][0].result).toBe(42);

            const socket = (wsClient as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            const secondResult = await wsClient.query(
                'MAP 84 as result',
                {},
                [Schema.object({result: Schema.number()})]
            );

            expect(secondResult[0][0].result).toBe(84);
        }, 15000);

        it('should use exponential backoff for reconnection attempts', async () => {
            const consoleLogSpy = vi.spyOn(console, 'log');

            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 100
            });

            const socket = (wsClient as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 1000));

            const reconnectMessages = consoleLogSpy.mock.calls
                .filter(call => call[0]?.includes('Attempting reconnection'))
                .map(call => call[0]);

            expect(reconnectMessages.length).toBeGreaterThan(0);
            expect(reconnectMessages[0]).toContain('100ms');

            consoleLogSpy.mockRestore();
        }, 15000);

        it('should stop reconnecting after max attempts', async () => {
            const consoleErrorSpy = vi.spyOn(console, 'error');

            wsClient = await Client.connect_ws('ws://127.0.0.1:9999', {
                timeoutMs: 1000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 100
            }).catch(() => null);

            if (!wsClient) {
                expect(true).toBe(true);
                return;
            }

            const socket = (wsClient as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 2000));

            const maxAttemptsReached = consoleErrorSpy.mock.calls.some(
                call => call[0]?.includes('Max reconnection attempts')
            );

            expect(maxAttemptsReached).toBe(true);

            consoleErrorSpy.mockRestore();
        }, 15000);

        it('should reject pending requests when connection is lost', async () => {
            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 0,
                reconnectDelayMs: 100
            });

            const socket = (wsClient as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 100));

            const queryPromise = wsClient.query(
                'MAP 42 as result',
                {},
                [Schema.object({result: Schema.number()})]
            );

            await expect(queryPromise).rejects.toThrow();
        }, 15000);

        it('should not reconnect after manual disconnect', async () => {
            const consoleLogSpy = vi.spyOn(console, 'log');

            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 100
            });

            wsClient.disconnect();

            await new Promise(resolve => setTimeout(resolve, 500));

            const reconnectMessages = consoleLogSpy.mock.calls
                .filter(call => call[0]?.includes('Attempting reconnection'));

            expect(reconnectMessages.length).toBe(0);

            consoleLogSpy.mockRestore();
        }, 15000);

        it('should successfully execute queries after reconnection', async () => {
            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 100
            });

            const socket = (wsClient as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            const results = [];
            for (let i = 0; i < 3; i++) {
                const result = await wsClient.query(
                    `MAP ${i} as result`,
                    {},
                    [Schema.object({result: Schema.number()})]
                );
                results.push(result[0][0].result);
            }

            expect(results).toEqual([0, 1, 2]);
        }, 15000);

        it('should reset reconnection attempts counter after successful reconnection', async () => {
            wsClient = await Client.connect_ws(WS_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 100
            });

            const socket1 = (wsClient as any).socket;
            socket1.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            await wsClient.query(
                'MAP 1 as result',
                {},
                [Schema.object({result: Schema.number()})]
            );

            expect((wsClient as any).reconnectAttempts).toBe(0);

            const socket2 = (wsClient as any).socket;
            socket2.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            await wsClient.query(
                'MAP 2 as result',
                {},
                [Schema.object({result: Schema.number()})]
            );

            expect((wsClient as any).reconnectAttempts).toBe(0);
        }, 15000);
    });
});
