// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, JsonWebsocketClient} from "../../../src";

describe('Concurrent requests', () => {
    let ws_client: JsonWebsocketClient;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);


    beforeEach(async () => {
        try {
            ws_client = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


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

    describe('admin', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                ws_client.admin('MAP {result: 1};'),
                ws_client.admin('MAP { a: 2, b: 3 };'),
                ws_client.admin("MAP {result: 'ReifyDB'};")
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
                ws_client.command('MAP {result: 1};'),
                ws_client.command('MAP { a: 2, b: 3 };'),
                ws_client.command("MAP {result: 'ReifyDB'};")
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
                ws_client.query('MAP {result: 1};'),
                ws_client.query('MAP { a: 2, b: 3 };'),
                ws_client.query("MAP {result: 'ReifyDB'};")
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
                ws_client.admin('MAP {result: 1};'),
                ws_client.query('MAP { a: 2, b: 3 };'),
                ws_client.admin("MAP {result: 'ReifyDB'};")
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
                ws_client.command('MAP {result: 1};'),
                ws_client.query('MAP { a: 2, b: 3 };'),
                ws_client.command("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });
});
