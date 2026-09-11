// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {waitForDatabase} from "../setup";
import {Client, JsonWsClient} from "../../../src";
import {expectSingleValueResult} from "./test-helper";
import {
    BooleanValue, Int1Value, Int2Value, Int4Value, Int8Value,
    Uint1Value, Uint16Value, Float8Value, Utf8Value, BlobValue,
    DateTimeValue, Uuid4Value, Uuid7Value
} from "@reifydb/core";

describe('Positional Parameters (Primitive)', () => {
    let wsClient: JsonWsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        try {
            wsClient = await Client.connectJsonWs(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN,
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            try { wsClient.disconnect(); } catch (error) { console.error('Error during disconnect:', error); }
            wsClient = null;
        }
    });

    describe('admin', () => {

        it('Boolean', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [true]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [42]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [1234]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [12345678]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [BigInt("42")]
            );

            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [BigInt("170141183460469231731687303715884105727")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [255]
            );

            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [65535]
            );

            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [4294967295]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [BigInt("255")]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [BigInt("340282366920938463463374607431768211455")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [3.14]
            );

            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [3.141592653589793]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                ["123.456789"]
            );

            expectSingleValueResult(frames, new Utf8Value("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                ["Hello, World!"]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [data]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [date]
            );

            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [time]
            );

            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [datetime]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                ["P1DT2H30M"]
            );

            expectSingleValueResult(frames, new Utf8Value("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [identityId]
            );

            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [true]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [42]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [1234]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [12345678]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [BigInt("42")]
            );

            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [BigInt("170141183460469231731687303715884105727")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [255]
            );

            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [65535]
            );

            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [4294967295]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [BigInt("255")]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [BigInt("340282366920938463463374607431768211455")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [3.14]
            );

            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [3.141592653589793]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                ["123.456789"]
            );

            expectSingleValueResult(frames, new Utf8Value("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                ["Hello, World!"]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [data]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [date]
            );

            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [time]
            );

            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [datetime]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                ["P1DT2H30M"]
            );

            expectSingleValueResult(frames, new Utf8Value("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [identityId]
            );

            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [true]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [42]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [1234]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [12345678]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [BigInt("42")]
            );

            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [BigInt("170141183460469231731687303715884105727")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [255]
            );

            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [65535]
            );

            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [4294967295]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [BigInt("255")]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [BigInt("340282366920938463463374607431768211455")]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [3.14]
            );

            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [3.141592653589793]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                ["123.456789"]
            );

            expectSingleValueResult(frames, new Utf8Value("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                ["Hello, World!"]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [data]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [date]
            );

            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [time]
            );

            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [datetime]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                ["P1DT2H30M"]
            );

            expectSingleValueResult(frames, new Utf8Value("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [uuid]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [identityId]
            );

            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

});
