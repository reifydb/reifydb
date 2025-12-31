// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Schema} from "@reifydb/core";

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
                wsClient.command(
                    'MAP 1 as result;',
                    {},
                    [Schema.object({result: Schema.int4Value()})]
                ),
                wsClient.command(
                    'MAP { 2 as a, 3 as b };',
                    {},
                    [Schema.object({a: Schema.int4Value(), b: Schema.int4Value()})]
                ),
                wsClient.command(
                    "MAP 'ReifyDB' as result;",
                    {},
                    [Schema.object({result: Schema.utf8Value()})]
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
                wsClient.query(
                    'MAP 1 as result;',
                    {},
                    [Schema.object({result: Schema.int4Value()})]
                ),
                wsClient.query(
                    'MAP { 2 as a, 3 as b };',
                    {},
                    [Schema.object({a: Schema.int4Value(), b: Schema.int4Value()})]
                ),
                wsClient.query(
                    "MAP 'ReifyDB' as result;",
                    {},
                    [Schema.object({result: Schema.utf8Value()})]
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
                wsClient.command(
                    'MAP 1 as result;',
                    {},
                    [Schema.object({result: Schema.int4Value()})]
                ),
                wsClient.query(
                    'MAP { 2 as a, 3 as b };',
                    {},
                    [Schema.object({a: Schema.int4Value(), b: Schema.int4Value()})]
                ),
                wsClient.command(
                    "MAP 'ReifyDB' as result;",
                    {},
                    [Schema.object({result: Schema.utf8Value()})]
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });
});
