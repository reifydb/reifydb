// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Client, HttpClient} from "../../../src";
import {Shape} from "@reifydb/core";

describe('Concurrent requests', () => {
    let httpClient: HttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    });

    describe('admin', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                httpClient.admin(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                httpClient.admin(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                httpClient.admin(
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
                httpClient.command(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                httpClient.command(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                httpClient.command(
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
                httpClient.query(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                httpClient.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                httpClient.query(
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
                httpClient.admin(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                httpClient.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                httpClient.admin(
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
                httpClient.command(
                    'MAP {result: 1};',
                    {},
                    [Shape.object({result: Shape.int4Value()})]
                ),
                httpClient.query(
                    'MAP { a: 2, b: 3 };',
                    {},
                    [Shape.object({a: Shape.int4Value(), b: Shape.int4Value()})]
                ),
                httpClient.command(
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
