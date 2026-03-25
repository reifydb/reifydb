// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWebsocketClient} from "../../../src";

describe('Auth Login Tests — JSON WebSocket', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('Password Authentication', () => {
        let client: JsonWebsocketClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should login with correct password and execute queries', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            const result = await client.loginWithPassword('alice', 'alice-pass');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}');
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong password', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('alice', 'wrong-password')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithPassword('nonexistent', 'password')).rejects.toThrow();
        }, 10000);
    });

    describe('Token Authentication', () => {
        let client: JsonWebsocketClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should login with correct token and execute queries', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            const result = await client.loginWithToken('bob', 'bob-secret-token');

            expect(result.token).toBeDefined();
            expect(result.token.length).toBeGreaterThan(0);
            expect(result.identity).toBeDefined();
            expect(result.identity.length).toBeGreaterThan(0);

            const frames = await client.query('MAP {v: 42}');
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].v).toBe(42);
        }, 10000);

        it('should reject wrong token', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('bob', 'wrong-token')).rejects.toThrow();
        }, 10000);

        it('should reject unknown user', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await expect(client.loginWithToken('nonexistent', 'some-token')).rejects.toThrow();
        }, 10000);
    });

    describe('Sequential Logins', () => {
        let client: JsonWebsocketClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should allow switching users via sequential logins', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});

            const resultA = await client.loginWithPassword('alice', 'alice-pass');
            expect(resultA.token).toBeDefined();

            const framesA = await client.query('MAP {v: 1}');
            expect(framesA[0][0].v).toBe(1);

            const resultB = await client.loginWithToken('bob', 'bob-secret-token');
            expect(resultB.token).toBeDefined();
            expect(resultB.token).not.toBe(resultA.token);

            const framesB = await client.query('MAP {v: 2}');
            expect(framesB[0][0].v).toBe(2);
        }, 10000);
    });

    describe('Logout', () => {
        let client: JsonWebsocketClient;

        afterEach(async () => {
            if (client) {
                try { client.disconnect(); } catch {}
                client = null;
            }
        });

        it('should logout and revoke token', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            const result = await client.loginWithPassword('alice', 'alice-pass');
            const oldToken = result.token;

            const frames = await client.query('MAP {v: 1}');
            expect(frames[0][0].v).toBe(1);

            await client.logout();

            // Verify the old token is revoked server-side
            const client2 = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000, token: oldToken});
            await expect(client2.query('MAP {v: 2}')).rejects.toThrow();
            client2.disconnect();
        }, 10000);

        it('should handle double logout', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await client.loginWithPassword('alice', 'alice-pass');

            await client.logout();
            await client.logout();
        }, 10000);

        it('should handle logout without token', async () => {
            client = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            await client.logout();
        }, 10000);

        it('should not affect other sessions', async () => {
            const clientA = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});
            const clientB = await Client.connect_json_ws(WS_URL, {timeoutMs: 10000});

            await clientA.loginWithPassword('alice', 'alice-pass');
            await clientB.loginWithPassword('alice', 'alice-pass');

            await clientA.logout();
            clientA.disconnect();

            const frames = await clientB.query('MAP {v: 42}');
            expect(frames[0][0].v).toBe(42);

            clientB.disconnect();
            client = null;
        }, 10000);
    });
});
