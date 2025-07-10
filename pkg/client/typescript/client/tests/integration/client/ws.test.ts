/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterAll,it, expect, afterEach, beforeAll, beforeEach, describe} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client} from "../../../src";
import {WsClient} from "../../../src/ws";

describe('ReifyDB Client Integration Tests', () => {
    const WS_URL = process.env.REIFYDB_WS_URL;
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    });

    afterAll(async () => {
        await wsClient.disconnect();
    });

    describe('WebSocket Client', () => {
        beforeEach(async () => {
            wsClient = await Client.connect_ws(WS_URL, {
                token: AUTH_TOKEN,
                timeoutMs: 5_000
            });
        });

        afterEach(() => {
            if (wsClient) wsClient.disconnect();
        });

        it('should execute simple tx', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        });

        it('should execute simple rx', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT 42 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        });
    })
})