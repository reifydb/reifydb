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

    describe('write', () => {
        it('out of range', async () => {
            await expect(
                wsClient.write<[{ result: number }]>(
                    "MAP cast(129, int1) as result;"
                )
            ).rejects.toMatchObject({
                name: 'ReifyError',
                code: 'CAST_002',
                cause: expect.objectContaining({
                    code: expect.stringContaining('NUMBER_002'),
                    label: expect.stringContaining("value '129' exceeds the valid range for type INT1 (-128 to 127)"),
                    message: expect.stringContaining('number out of range')
                })
            });

        }, 1000);
    });


    describe('read', () => {
        it('out of range', async () => {
            await expect(
                wsClient.read<[{ result: number }]>(
                    "MAP cast(129, int1) as result;"
                )
            ).rejects.toMatchObject({
                name: 'ReifyError',
                code: 'CAST_002',
                cause: expect.objectContaining({
                    code: expect.stringContaining('NUMBER_002'),
                    label: expect.stringContaining("value '129' exceeds the valid range for type INT1 (-128 to 127)"),
                    message: expect.stringContaining('number out of range')
                })
            });

        }, 1000);
    });

});
