// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Client, HttpClient} from "../../../src";
import {Shape} from "@reifydb/core";


describe.each([
    {encoding: "json"},
    {encoding: "rbcf"},
] as const)('Error [$encoding]', ({encoding}) => {
    let httpClient: HttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
            encoding,
        });
    });

    describe('admin', () => {
        it('out of range', async () => {
            await expect(
                httpClient.admin(
                    "MAP {result: cast(129, int1)};",
                    {},
                    [Shape.object({result: Shape.int1Value()})]
                )
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
                httpClient.command(
                    "MAP {result: cast(129, int1)};",
                    {},
                    [Shape.object({result: Shape.int1Value()})]
                )
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
                httpClient.query(
                    "MAP {result: cast(129, int1)};",
                    {},
                    [Shape.object({result: Shape.int1Value()})]
                )
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
