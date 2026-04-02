// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Shape} from "@reifydb/core";
import {Client, HttpClient} from "../../../src";

describe('ReifyDB Client Integration Tests', () => {
    const HTTP_URL = process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    describe('HTTP Client', () => {
        let httpClient: HttpClient;

        beforeAll(async () => {
            httpClient = Client.connect_http(HTTP_URL, {
                timeoutMs: 10000,
                token: AUTH_TOKEN
            });
        });

        it('should execute simple command', async () => {
            const frames = await httpClient.command(
                'MAP {result: 42}',
                {},
                [
                    Shape.object({result: Shape.number()}),
                ]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            expect(frames[0][0].result).toBe(42);
        }, 10000);

        it('should execute simple query', async () => {
            const frames = await httpClient.query(
                'MAP {result: 42}',
                {},
                [
                    Shape.object({result: Shape.number()}),
                ]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            expect(frames[0][0].result).toBe(42);
        }, 10000);
    });
});
