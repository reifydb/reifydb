/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";

describe('Concurrent requests', () => {
    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);


    beforeEach(async () => {
        try {
            wsClient = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


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

    describe('tx', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.tx<[{ result: number }]>(
                    'SELECT 1 as result;'
                ),
                wsClient.tx<[{ a: number, b: number }]>(
                    'SELECT 2 as a, 3 as b;'
                ),
                wsClient.tx<[{ result: string }]>(
                    "SELECT 'ReifyDB' as result;"
                )
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });


    describe('rx', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.rx<[{ result: number }]>(
                    'SELECT 1 as result;'
                ),
                wsClient.rx<[{ a: number, b: number }]>(
                    'SELECT 2 as a, 3 as b;'
                ),
                wsClient.rx<[{ result: string }]>(
                    "SELECT 'ReifyDB' as result;"
                )
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });

    describe('tx & rx mixed', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.tx<[{ result: number }]>(
                    'SELECT 1 as result;'
                ),
                wsClient.rx<[{ a: number, b: number }]>(
                    'SELECT 2 as a, 3 as b;'
                ),
                wsClient.tx<[{ result: string }]>(
                    "SELECT 'ReifyDB' as result;"
                )
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });
});
