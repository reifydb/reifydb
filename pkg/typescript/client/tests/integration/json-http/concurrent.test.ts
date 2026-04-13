// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Client, JsonHttpClient} from "../../../src";

describe('Concurrent requests', () => {
    let httpClient: JsonHttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_json_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
        });
    });

    describe('admin', () => {
        it('should handle multiple concurrent requests', async () => {
            const [result1, result2, result3] = await Promise.all([
                httpClient.admin('MAP {result: 1};'),
                httpClient.admin('MAP { a: 2, b: 3 };'),
                httpClient.admin("MAP {result: 'ReifyDB'};")
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
                httpClient.command('MAP {result: 1};'),
                httpClient.command('MAP { a: 2, b: 3 };'),
                httpClient.command("MAP {result: 'ReifyDB'};")
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
                httpClient.query('MAP {result: 1};'),
                httpClient.query('MAP { a: 2, b: 3 };'),
                httpClient.query("MAP {result: 'ReifyDB'};")
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
                httpClient.admin('MAP {result: 1};'),
                httpClient.query('MAP { a: 2, b: 3 };'),
                httpClient.admin("MAP {result: 'ReifyDB'};")
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
                httpClient.command('MAP {result: 1};'),
                httpClient.query('MAP { a: 2, b: 3 };'),
                httpClient.command("MAP {result: 'ReifyDB'};")
            ]);

            expect(result1[0][0].result).toBe(1);
            expect(result2[0][0].a).toBe(2);
            expect(result2[0][0].b).toBe(3);
            expect(result3[0][0].result).toBe('ReifyDB');
        });
    });
});
