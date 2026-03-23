// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWebsocketClient} from "../../../src";

describe('Concurrent requests', () => {
    let wsClient: JsonWebsocketClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);


    beforeEach(async () => {
        try {
            wsClient = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


    afterEach(async () => {
        if (wsClient) {
            try {
                wsClient.disconnect();
            } catch (error) {
                console.error('Error during disconnect:', error);
            }
            wsClient = null;
        }
    });

    describe('admin', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.admin('MAP {result: 1};'),
                wsClient.admin('MAP { a: 2, b: 3 };'),
                wsClient.admin("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });

    describe('command', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.command('MAP {result: 1};'),
                wsClient.command('MAP { a: 2, b: 3 };'),
                wsClient.command("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });


    describe('query', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.query('MAP {result: 1};'),
                wsClient.query('MAP { a: 2, b: 3 };'),
                wsClient.query("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });

    describe('admin & query mixed', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.admin('MAP {result: 1};'),
                wsClient.query('MAP { a: 2, b: 3 };'),
                wsClient.admin("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });

    describe('command & query mixed', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.command('MAP {result: 1};'),
                wsClient.query('MAP { a: 2, b: 3 };'),
                wsClient.command("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });
});
