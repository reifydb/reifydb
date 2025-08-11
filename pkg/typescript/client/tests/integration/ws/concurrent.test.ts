/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient, Int4Value, Utf8Value} from "../../../src";
import { LEGACY_SCHEMA } from "../test-helpers";

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

    describe('command', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.command<[{ result: Int4Value }]>(
                    'MAP 1 as result;',
                    LEGACY_SCHEMA
                ),
                wsClient.command<[{ a: Int4Value, b: Int4Value }]>(
                    'MAP { 2 as a, 3 as b };',
                    LEGACY_SCHEMA
                ),
                wsClient.command<[{ result: Utf8Value }]>(
                    "MAP 'ReifyDB' as result;",
                    LEGACY_SCHEMA
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });


    describe('query', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.query<[{ result: Int4Value }]>(
                    'MAP 1 as result;',
                    LEGACY_SCHEMA
                ),
                wsClient.query<[{ a: Int4Value, b: Int4Value }]>(
                    'MAP { 2 as a, 3 as b };',
                    LEGACY_SCHEMA
                ),
                wsClient.query<[{ result: Utf8Value }]>(
                    "MAP 'ReifyDB' as result;",
                    LEGACY_SCHEMA
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });

    describe('command & query mixed', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                wsClient.command<[{ result: Int4Value }]>(
                    'MAP 1 as result;',
                    LEGACY_SCHEMA
                ),
                wsClient.query<[{ a: Int4Value, b: Int4Value }]>(
                    'MAP { 2 as a, 3 as b };',
                    LEGACY_SCHEMA
                ),
                wsClient.command<[{ result: Utf8Value }]>(
                    "MAP 'ReifyDB' as result;",
                    LEGACY_SCHEMA
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });
});
