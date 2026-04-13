// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, WsClient} from "../../../src";
import {Shape} from "@reifydb/core";

describe.each([
    {format: "frames"},
    {format: "rbcf"},
] as const)('Concurrent requests [$format]', ({format}) => {
    let ws_client: WsClient;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);


    beforeEach(async () => {
        try {
            ws_client = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN,
                format,
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


    afterEach(async () => {
        if (ws_client) {
            try {
                ws_client.disconnect();
            } catch (error) {
                console.error('⚠️ Error during disconnect:', error);
            }
            ws_client = null;
        }
    });

    describe('admin', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                ws_client.admin(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                ws_client.admin(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                ws_client.admin(
                    "MAP {result: 'ReifyDB'};",
                    {},
                    [Shape.object({result: Shape.utf8Value()})]
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });

    describe('command', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                ws_client.command(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                ws_client.command(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                ws_client.command(
                    "MAP {result: 'ReifyDB'};",
                    {},
                    [Shape.object({result: Shape.utf8Value()})]
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
                ws_client.query(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                ws_client.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                ws_client.query(
                    "MAP {result: 'ReifyDB'};",
                    {},
                    [Shape.object({result: Shape.utf8Value()})]
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });

    describe('admin & query mixed', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                ws_client.admin(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                ws_client.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                ws_client.admin(
                    "MAP {result: 'ReifyDB'};",
                    {},
                    [Shape.object({result: Shape.utf8Value()})]
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
                ws_client.command(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                ws_client.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                ws_client.command(
                    "MAP {result: 'ReifyDB'};",
                    {},
                    [Shape.object({result: Shape.utf8Value()})]
                )
            ]);

            expect(result1[0][0].result.value).toBe(1);
            expect(result2[0][0].a.value).toBe(2);
            expect(result2[0][0].b.value).toBe(3);
            expect(result3[0][0].result.value).toBe('ReifyDB');
        });
    });
});
