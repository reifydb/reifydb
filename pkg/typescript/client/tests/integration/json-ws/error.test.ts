// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, JsonWsClient} from "../../../src";


describe.each([
    {format: "json"},
    {format: "rbcf"},
] as const)('Error [$format]', ({format}) => {
    let ws_client: JsonWsClient;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);


    beforeEach(async () => {
        try {
            ws_client = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN,
                format,
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

    afterEach(async () => {
        if (ws_client) {
            try {
                ws_client.disconnect();
            } catch (error) {
                console.error('Error during disconnect:', error);
            }
            ws_client = null;
        }
    });

    describe('admin', () => {
        it('out of range', async () => {
            await expect(
                ws_client.admin("MAP {result: cast(129, int1)};")
            ).rejects.toMatchObject({
                name: 'ReifyError',
                code: 'CAST_002',
                statement: "MAP {result: cast(129, int1)};",
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

    describe('command', () => {
        it('out of range', async () => {
            await expect(
                ws_client.command("MAP {result: cast(129, int1)};")
            ).rejects.toMatchObject({
                name: 'ReifyError',
                code: 'CAST_002',
                statement: "MAP {result: cast(129, int1)};",
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


    describe('query', () => {
        it('out of range', async () => {
            await expect(
                ws_client.query("MAP {result: cast(129, int1)};")
            ).rejects.toMatchObject({
                name: 'ReifyError',
                code: 'CAST_002',
                statement: "MAP {result: cast(129, int1)};",
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

});
