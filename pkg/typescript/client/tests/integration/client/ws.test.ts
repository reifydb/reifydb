// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Shape} from "@reifydb/core";
import {Client, WsClient} from "../../../src";

describe('ReifyDB Client Integration Tests', () => {
    const WS_URL = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:18090';
    const AUTH_TOKEN = process.env.REIFYDB_TOKEN;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    describe('WebSocket Client', () => {
        let ws_client: WsClient;

        beforeEach(async () => {
            try {
                ws_client = await Client.connect_ws(WS_URL, {
                    timeout_ms: 10000,
                    token: AUTH_TOKEN
                });
            } catch (error) {
                console.error('❌ WebSocket connection failed:', error);
                throw error;
            }
        }, 15000); // 15 second timeout

        afterEach(async () => {
            if (ws_client) {
                try {
                    ws_client.disconnect();
                } catch (error) {
                    console.error('⚠️ Error during disconnect:', error);
                }
                ws_client = null;
            }
        });

        it('should execute simple command', async () => {
            const frames = await ws_client.command(
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
            const frames = await ws_client.query(
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