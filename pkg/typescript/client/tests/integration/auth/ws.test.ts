// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('Auth Login Tests — WebSocket', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';

    beforeAll(async () => {
        await wait_for_database();
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
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            const result = await client.login_with_password('alice', 'alice-pass');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong password', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            await expect(client.login_with_password('alice', 'wrong-password')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            await expect(client.login_with_password('nonexistent', 'password')).rejects.toThrow();
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
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            const result = await client.login_with_token('bob-secret-token');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong token', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            await expect(client.login_with_token('wrong-token')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            await expect(client.login_with_token('some-token')).rejects.toThrow();
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
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});

            // Login as alice
            const resultA = await client.login_with_password('alice', 'alice-pass');
            expect(resultA.token).toBeDefined();

            // Verify query works as alice
            const framesA = await client.query('MAP {v: 1}', {}, [Shape.object({v: Shape.number()})]);
            expect(framesA[0][0].v).toBe(1);

            // Login as bob (replaces alice session)
            const resultB = await client.login_with_token('bob-secret-token');
            expect(resultB.token).toBeDefined();
            expect(resultB.token).not.toBe(resultA.token);

            // Verify query works as bob
            const framesB = await client.query('MAP {v: 2}', {}, [Shape.object({v: Shape.number()})]);
            expect(framesB[0][0].v).toBe(2);
        }, 10000);
    });

    describe('Logout', () => {
        let client: WsClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should logout and revoke token', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            const result = await client.login_with_password('alice', 'alice-pass');
            const oldToken = result.token;

            const frames = await client.query('MAP {v: 1}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(1);

            await client.logout();

            // Verify the old token is revoked server-side
            const client2 = await Client.connect_ws(WS_URL, {timeout_ms: 10000, token: oldToken});
            await expect(client2.query('MAP {v: 2}', {}, [Shape.object({v: Shape.number()})])).rejects.toThrow();
            client2.disconnect();
        }, 10000);

        it('should handle double logout', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            await client.login_with_password('alice', 'alice-pass');

            await client.logout();
            // Second logout is a no-op (no token)
            await client.logout();
        }, 10000);

        it('should handle logout without token', async () => {
            client = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            // No login — logout should be a no-op
            await client.logout();
        }, 10000);

        it('should not affect other sessions', async () => {
            const clientA = await Client.connect_ws(WS_URL, {timeout_ms: 10000});
            const clientB = await Client.connect_ws(WS_URL, {timeout_ms: 10000});

            await clientA.login_with_password('alice', 'alice-pass');
            await clientB.login_with_password('alice', 'alice-pass');

            await clientA.logout();
            clientA.disconnect();

            // clientB should still work
            const frames = await clientB.query('MAP {v: 42}', {}, [Shape.object({v: Shape.number()})]);
            expect(frames[0][0].v).toBe(42);

            clientB.disconnect();
            client = null; // already cleaned up
        }, 10000);
    });
});
