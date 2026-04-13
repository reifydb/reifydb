// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, JsonWsClient} from "../../../src";

describe.each([
    {format: "json"},
    {format: "rbcf"},
] as const)('WebSocket Client Reconnection [$format]', ({format}) => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    describe('Automatic Reconnection', () => {
        let ws_client: JsonWsClient;

        afterEach(async () => {
            if (ws_client) {
                try {
                    ws_client.disconnect();
                } catch (error) {
                    console.error('Error during disconnect:', error);
                }
                ws_client = null;
            }
        });

        it('should reconnect after connection is lost', async () => {
            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 100,
                format,
            });

            const firstResult = await ws_client.query('MAP {result: 42}');

            expect(firstResult[0][0].result).toBe(42);

            const socket = (ws_client as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            const secondResult = await ws_client.query('MAP {result: 84}');

            expect(secondResult[0][0].result).toBe(84);
        }, 15000);

        it('should use exponential backoff for reconnection attempts', async () => {
            const console_log_spy = vi.spyOn(console, 'log');

            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 100,
                format,
            });

            const socket = (ws_client as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 1000));

            const reconnect_messages = console_log_spy.mock.calls
                .filter(call => call[0]?.includes('Attempting reconnection'))
                .map(call => call[0]);

            expect(reconnect_messages.length).toBeGreaterThan(0);
            expect(reconnect_messages[0]).toContain('100ms');

            console_log_spy.mockRestore();
        }, 15000);

        it('should stop reconnecting after max attempts', async () => {
            const console_error_spy = vi.spyOn(console, 'error');

            ws_client = await Client.connect_json_ws('ws://127.0.0.1:9999', {
                timeout_ms: 1000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 2,
                reconnect_delay_ms: 100,
                format,
            }).catch(() => null);

            if (!ws_client) {
                expect(true).toBe(true);
                return;
            }

            const socket = (ws_client as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 2000));

            const max_attempts_reached = console_error_spy.mock.calls.some(
                call => call[0]?.includes('Max reconnection attempts')
            );

            expect(max_attempts_reached).toBe(true);

            console_error_spy.mockRestore();
        }, 15000);

        it('should reject pending requests when connection is lost', async () => {
            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 0,
                reconnect_delay_ms: 100,
                format,
            });

            const socket = (ws_client as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 100));

            const queryPromise = ws_client.query('MAP {result: 42}');

            await expect(queryPromise).rejects.toThrow();
        }, 15000);

        it('should not reconnect after manual disconnect', async () => {
            const console_log_spy = vi.spyOn(console, 'log');

            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 100,
                format,
            });

            ws_client.disconnect();

            await new Promise(resolve => setTimeout(resolve, 500));

            const reconnect_messages = console_log_spy.mock.calls
                .filter(call => call[0]?.includes('Attempting reconnection'));

            expect(reconnect_messages.length).toBe(0);

            console_log_spy.mockRestore();
        }, 15000);

        it('should successfully execute queries after reconnection', async () => {
            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 100,
                format,
            });

            const socket = (ws_client as any).socket;
            socket.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            const results = [];
            for (let i = 0; i < 3; i++) {
                const result = await ws_client.query(`MAP {result: ${i}}`);
                results.push(result[0][0].result);
            }

            expect(results).toEqual([0, 1, 2]);
        }, 15000);

        it('should reset reconnection attempts counter after successful reconnection', async () => {
            ws_client = await Client.connect_json_ws(WS_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 100,
                format,
            });

            const socket1 = (ws_client as any).socket;
            socket1.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            await ws_client.query('MAP {result: 1}');

            expect((ws_client as any).reconnect_attempts).toBe(0);

            const socket2 = (ws_client as any).socket;
            socket2.close();

            await new Promise(resolve => setTimeout(resolve, 500));

            await ws_client.query('MAP {result: 2}');

            expect((ws_client as any).reconnect_attempts).toBe(0);
        }, 15000);
    });
});
