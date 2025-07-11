/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";

describe('ReifyDB Client Integration Tests', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    describe('WebSocket Client', () => {
        let wsClient: WsClient;

        beforeEach(async () => {
            try {
                wsClient = await Client.connect_ws(WS_URL, {
                    timeoutMs: 10000,
                    token: AUTH_TOKEN
                });
            } catch (error) {
                console.error('❌ WebSocket connection failed:', error);
                throw error;
            }
        }, 15000); // 15 second timeout

        afterEach(async () => {
            if (wsClient) {
                try {
                    wsClient.disconnect();
                } catch (error) {
                    console.error('⚠️ Error during disconnect:', error);
                }
                wsClient = null;
            }
        });

        it('should execute simple tx', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'MAP 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);

        it('should execute simple rx', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'MAP 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 10000);
    });
});