/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {Client, WsClient} from "../../../src";
import {waitForDatabase} from "../setup";
import {Schema} from "@reifydb/core";
import {
    expectSingleResult,
    expectSingleDateResult,
    expectSingleBlobResult,
    expectSingleBigIntResult
} from "./test-helper";

describe('Positional Parameters', () => {
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

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [true],
                [Schema.object({result: Schema.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [42],
                [Schema.object({result: Schema.int1()})]
            );

            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [1234],
                [Schema.object({result: Schema.int2()})]
            );

            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [12345678],
                [Schema.object({result: Schema.int4()})]
            );

            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [BigInt("9223372036854775807")],
                [Schema.object({result: Schema.int8()})]
            );

            expectSingleResult(frames, BigInt("9223372036854775807"), 'bigint');
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [BigInt("170141183460469231731687303715884105727")],
                [Schema.object({result: Schema.int16()})]
            );

            expectSingleResult(frames, BigInt("170141183460469231731687303715884105727"), 'bigint');
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [255],
                [Schema.object({result: Schema.uint1()})]
            );

            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [65535],
                [Schema.object({result: Schema.uint2()})]
            );

            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [4294967295],
                [Schema.object({result: Schema.uint4()})]
            );

            expectSingleResult(frames, BigInt(4294967295), 'bigint');
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [BigInt("18446744073709551615")],
                [Schema.object({result: Schema.uint8()})]
            );

            expectSingleResult(frames, BigInt("18446744073709551615"), 'bigint');
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [BigInt("340282366920938463463374607431768211455")],
                [Schema.object({result: Schema.uint16()})]
            );

            expectSingleResult(frames, BigInt("340282366920938463463374607431768211455"), 'bigint');
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [3.14],
                [Schema.object({result: Schema.float4()})]
            );

            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [3.141592653589793],
                [Schema.object({result: Schema.float8()})]
            );

            expectSingleResult(frames, 3.141592653589793, 'number');
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                ["Hello, World!"],
                [Schema.object({result: Schema.utf8()})]
            );

            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.command(
                'MAP $1 as result',
                [data],
                [Schema.object({result: Schema.blob()})]
            );

            expectSingleBlobResult(frames, data);
        }, 1000);

        it('RowNumber', async () => {
            const frames = await wsClient.command(
                'MAP $1 as result',
                [BigInt("123456789")],
                [Schema.object({result: Schema.rownumber()})]
            );

            expectSingleBigIntResult(frames, BigInt(123456789));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.command(
                'MAP $1 as result',
                [date],
                [Schema.object({result: Schema.date()})]
            );

            expectSingleDateResult(frames, date);
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP $1 as result',
                [time],
                [Schema.object({result: Schema.time()})]
            );

            expectSingleDateResult(frames, time);
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP $1 as result',
                [datetime],
                [Schema.object({result: Schema.datetime()})]
            );

            expectSingleDateResult(frames, datetime);
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await wsClient.command(
                'MAP $1 as result',
                [duration],
                [Schema.object({result: Schema.duration()})]
            );

            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.command(
                'MAP $1 as result',
                [uuid],
                [Schema.object({result: Schema.uuid4()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.command(
                'MAP $1 as result',
                [uuid],
                [Schema.object({result: Schema.uuid7()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.command(
                'MAP $1 as result',
                [identityId],
                [Schema.object({result: Schema.identityid()})]
            );

            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [true],
                [Schema.object({result: Schema.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [42],
                [Schema.object({result: Schema.int1()})]
            );

            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [1234],
                [Schema.object({result: Schema.int2()})]
            );

            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [12345678],
                [Schema.object({result: Schema.int4()})]
            );

            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [BigInt("9223372036854775807")],
                [Schema.object({result: Schema.int8()})]
            );

            expectSingleResult(frames, BigInt("9223372036854775807"), 'bigint');
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [BigInt("170141183460469231731687303715884105727")],
                [Schema.object({result: Schema.int16()})]
            );

            expectSingleResult(frames, BigInt("170141183460469231731687303715884105727"), 'bigint');
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [255],
                [Schema.object({result: Schema.uint1()})]
            );

            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [65535],
                [Schema.object({result: Schema.uint2()})]
            );

            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [4294967295],
                [Schema.object({result: Schema.uint4()})]
            );

            expectSingleResult(frames, BigInt(4294967295), 'bigint');
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [BigInt("18446744073709551615")],
                [Schema.object({result: Schema.uint8()})]
            );

            expectSingleResult(frames, BigInt("18446744073709551615"), 'bigint');
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [BigInt("340282366920938463463374607431768211455")],
                [Schema.object({result: Schema.uint16()})]
            );

            expectSingleResult(frames, BigInt("340282366920938463463374607431768211455"), 'bigint');
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [3.14],
                [Schema.object({result: Schema.float4()})]
            );

            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [3.141592653589793],
                [Schema.object({result: Schema.float8()})]
            );

            expectSingleResult(frames, 3.141592653589793, 'number');
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                ["Hello, World!"],
                [Schema.object({result: Schema.utf8()})]
            );

            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.query(
                'MAP $1 as result',
                [data],
                [Schema.object({result: Schema.blob()})]
            );

            expectSingleBlobResult(frames, data);
        }, 1000);

        it('RowNumber', async () => {
            const frames = await wsClient.query(
                'MAP $1 as result',
                [BigInt("123456789")],
                [Schema.object({result: Schema.rownumber()})]
            );

            expectSingleBigIntResult(frames, BigInt(123456789));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.query(
                'MAP $1 as result',
                [date],
                [Schema.object({result: Schema.date()})]
            );

            expectSingleDateResult(frames, date);
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP $1 as result',
                [time],
                [Schema.object({result: Schema.time()})]
            );

            expectSingleDateResult(frames, time);
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP $1 as result',
                [datetime],
                [Schema.object({result: Schema.datetime()})]
            );

            expectSingleDateResult(frames, datetime);
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await wsClient.query(
                'MAP $1 as result',
                [duration],
                [Schema.object({result: Schema.duration()})]
            );

            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.query(
                'MAP $1 as result',
                [uuid],
                [Schema.object({result: Schema.uuid4()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.query(
                'MAP $1 as result',
                [uuid],
                [Schema.object({result: Schema.uuid7()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.query(
                'MAP $1 as result',
                [identityId],
                [Schema.object({result: Schema.identityid()})]
            );

            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

});