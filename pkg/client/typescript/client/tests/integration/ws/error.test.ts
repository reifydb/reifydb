/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";

describe('Error', () => {
    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);


    beforeEach(async () => {
        try {
            wsClient = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

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

    describe('tx', () => {
        it('out of range', async () => {
            await expect(
                wsClient.tx<[{ result: number }]>(
                    "SELECT cast(129, int1) as result;"
                )
            ).rejects.toMatchObject({
                name: 'ReifyError',
                message: expect.stringContaining('value out of range in type `INT1`')
            });
        }, 10);
    });


    describe('rx', () => {
        it('out of range', async () => {
            await expect(
                wsClient.rx<[{ result: number }]>(
                    "SELECT cast(129, int1) as result;"
                )
            ).rejects.toMatchObject({
                name: 'ReifyError',
                message: expect.stringContaining('value out of range in type `INT1`')
            });
        }, 10);
    });

});
