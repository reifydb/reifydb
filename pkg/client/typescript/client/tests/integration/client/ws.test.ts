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
        console.log('🔄 Waiting for database...');
        await waitForDatabase();
        console.log('✅ Database ready');
    }, 30000);

    describe('WebSocket Client', () => {
        let wsClient: WsClient;

        beforeEach(async () => {
            console.log('🔌 Connecting to WebSocket...');
            try {
                wsClient = await Client.connect_ws(WS_URL, {
                    timeoutMs: 10000,
                    token: AUTH_TOKEN
                });
                console.log('✅ WebSocket connected');
            } catch (error) {
                console.error('❌ WebSocket connection failed:', error);
                throw error;
            }
        }, 15000); // 15 second timeout

        afterEach(async () => {
            if (wsClient) {
                console.log('🔌 Disconnecting WebSocket...');
                try {
                    wsClient.disconnect();
                    console.log('✅ WebSocket disconnected');
                } catch (error) {
                    console.error('⚠️ Error during disconnect:', error);
                }
                wsClient = null;
            }
        });

        it('should execute simple tx', async () => {
            console.log('🧪 Running tx test...');
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
            console.log('✅ tx test passed');
        }, 10000);

        it('should execute simple rx', async () => {
            console.log('🧪 Running rx test...');
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
            console.log('✅ rx test passed');
        }, 10000);
    });
});