// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Schema} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('Auth Login Tests — WebSocket', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('Password Authentication', () => {
        let client: WsClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should login with correct password and execute queries', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            const result = await client.loginWithPassword('alice', 'alice-pass');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Schema.object({v: Schema.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong password', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('alice', 'wrong-password')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('nonexistent', 'password')).rejects.toThrow();
        }, 10000);
    });

    describe('Token Authentication', () => {
        let client: WsClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should login with correct token and execute queries', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            const result = await client.loginWithToken('bob', 'bob-secret-token');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Schema.object({v: Schema.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong token', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('bob', 'wrong-token')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('nonexistent', 'some-token')).rejects.toThrow();
        }, 10000);
    });

    describe('Sequential Logins', () => {
        let client: WsClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should allow switching users via sequential logins', async () => {
            client = await Client.connect_ws(WS_URL, {timeoutMs: 10000});

            // Login as alice
            const resultA = await client.loginWithPassword('alice', 'alice-pass');
            expect(resultA.token).toBeDefined();

            // Verify query works as alice
            const framesA = await client.query('MAP {v: 1}', {}, [Schema.object({v: Schema.number()})]);
            expect(framesA[0][0].v).toBe(1);

            // Login as bob (replaces alice session)
            const resultB = await client.loginWithToken('bob', 'bob-secret-token');
            expect(resultB.token).toBeDefined();
            expect(resultB.token).not.toBe(resultA.token);

            // Verify query works as bob
            const framesB = await client.query('MAP {v: 2}', {}, [Schema.object({v: Schema.number()})]);
            expect(framesB[0][0].v).toBe(2);
        }, 10000);
    });
});
