// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Client, JsonHttpClient} from "../../../src";

describe('ReifyDB Client Integration Tests', () => {
    const HTTP_URL = process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    describe('JSON HTTP Client', () => {
        let httpClient: JsonHttpClient;

        beforeAll(async () => {
            httpClient = Client.connect_json_http(HTTP_URL, {
                timeout_ms: 10000,
                token: AUTH_TOKEN
            });
        });

        it('should execute simple command', async () => {
            const frames = await httpClient.command('MAP {result: 42}');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);

        it('should execute simple query', async () => {
            const frames = await httpClient.query('MAP {result: 42}');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);
    });
});
