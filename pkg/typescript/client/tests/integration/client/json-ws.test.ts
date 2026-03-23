// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWebsocketClient} from "../../../src";

describe('ReifyDB Client Integration Tests', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('JSON WebSocket Client', () => {
        let wsClient: JsonWebsocketClient;

        beforeEach(async () => {
            try {
                wsClient = await Client.connect_json_ws(WS_URL, {
                    timeoutMs: 10000,
                    token: AUTH_TOKEN
                });
            } catch (error) {
                console.error('WebSocket connection failed:', error);
                throw error;
            }
        }, 15000);

        afterEach(async () => {
            if (wsClient) {
                try {
                    wsClient.disconnect();
                } catch (error) {
                    console.error('Error during disconnect:', error);
                }
                wsClient = null;
            }
        });

        it('should execute simple command', async () => {
            const frames = await wsClient.command('MAP {result: 42}');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);

        it('should execute simple query', async () => {
            const frames = await wsClient.query('MAP {result: 42}');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);
    });
});
