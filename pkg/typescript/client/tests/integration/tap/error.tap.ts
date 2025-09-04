// File: /pkg/typescript/client/tests/integration/tap/error.tap.ts
// Generated from: error.tap.md
/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Schema} from "@reifydb/core";

describe('Error Handling', () => {
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

    it('command_out_of_range', async () => {
        await expect(
            wsClient.command(
                "MAP cast(129, int1) as result;",
                {},
                [Schema.object({result: Schema.int1Value()})]
            )
        ).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'CAST_002',
            statement: "MAP cast(129, int1) as result;",
            fragment: {
                Statement: expect.objectContaining({
                    text: "129",
                    line: expect.any(Number),
                    column: expect.any(Number)
                })
            },
            cause: expect.objectContaining({
                code: expect.stringContaining('NUMBER_002'),
                label: expect.stringContaining("value '129' exceeds the valid range for type Int1 (-128 to 127)"),
                message: expect.stringContaining('number out of range')
            })
        });
    }, 1000);

    it('query_out_of_range', async () => {
        await expect(
            wsClient.query(
                "MAP cast(129, int1) as result;",
                {},
                [Schema.object({result: Schema.int1Value()})]
            )
        ).rejects.toMatchObject({
            name: 'ReifyError',
            code: 'CAST_002',
            statement: "MAP cast(129, int1) as result;",
            fragment: {
                Statement: expect.objectContaining({
                    text: "129",
                    line: expect.any(Number),
                    column: expect.any(Number)
                })
            },
            cause: expect.objectContaining({
                code: expect.stringContaining('NUMBER_002'),
                label: expect.stringContaining("value '129' exceeds the valid range for type Int1 (-128 to 127)"),
                message: expect.stringContaining('number out of range')
            })
        });
    }, 1000);
});