// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, HttpClient} from "../../../src";

describe('Auth Login Tests — HTTP', () => {
    const HTTP_URL = process.env.REIFYDB_HTTP_URL || 'http://127.0.0.1:18091';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('Password Authentication', () => {
        it('should login with correct password and execute queries', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            const result = await client.loginWithPassword('alice', 'alice-pass');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong password', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('alice', 'wrong-password')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('nonexistent', 'password')).rejects.toThrow();
        }, 10000);
    });

    describe('Token Authentication', () => {
        it('should login with correct token and execute queries', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            const result = await client.loginWithToken('bob', 'bob-secret-token');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong token', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('bob', 'wrong-token')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('nonexistent', 'some-token')).rejects.toThrow();
        }, 10000);
    });

    describe('Sequential Logins', () => {
        it('should allow switching users via sequential logins', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});

            const resultA = await client.loginWithPassword('alice', 'alice-pass');
            expect(resultA.token).toBeDefined();

            const framesA = await client.query('MAP {v: 1}', {}, [Shape.object({v: Shape.number()})]);
            expect(framesA[0][0].v).toBe(1);

            const resultB = await client.loginWithToken('bob', 'bob-secret-token');
            expect(resultB.token).toBeDefined();
            expect(resultB.token).not.toBe(resultA.token);

            const framesB = await client.query('MAP {v: 2}', {}, [Shape.object({v: Shape.number()})]);
            expect(framesB[0][0].v).toBe(2);
        }, 10000);
    });

    describe('Logout', () => {
        it('should logout and revoke token', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            const result = await client.loginWithPassword('alice', 'alice-pass');
            const oldToken = result.token;

            const frames = await client.query('MAP {v: 1}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(1);

            await client.logout();

            // Verify the old token is revoked server-side
            const client2 = Client.connect_http(HTTP_URL, {timeoutMs: 10000, token: oldToken});
            await expect(client2.query('MAP {v: 2}', {}, [Shape.object({v: Shape.number()})])).rejects.toThrow();
        }, 10000);

        it('should handle double logout', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            await client.loginWithPassword('alice', 'alice-pass');

            await client.logout();
            // Second logout is a no-op (no token)
            await client.logout();
        }, 10000);

        it('should handle logout without token', async () => {
            const client = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            // No login — logout should be a no-op
            await client.logout();
        }, 10000);

        it('should not affect other sessions', async () => {
            const clientA = Client.connect_http(HTTP_URL, {timeoutMs: 10000});
            const clientB = Client.connect_http(HTTP_URL, {timeoutMs: 10000});

            await clientA.loginWithPassword('alice', 'alice-pass');
            await clientB.loginWithPassword('alice', 'alice-pass');

            await clientA.logout();

            // clientB should still work
            const frames = await clientB.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);
    });
});
