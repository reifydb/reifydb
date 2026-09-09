// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {waitForDatabase} from "../setup";
import {Client, JsonWsClient} from "../../../src";
import {expectSingleValueResult} from "./test-helper";
import {
    BooleanValue, Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    Float4Value, Float8Value, DecimalValue, Utf8Value, BlobValue,
    DateValue, TimeValue, DateTimeValue, DurationValue,
    Uuid4Value, Uuid7Value, NoneValue, IdentityIdValue
} from "@reifydb/core";

describe('Positional Parameters (Value)', () => {
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
                [new BooleanValue(true)]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Int1Value(42)]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Int2Value(1234)]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Int4Value(12345678)]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Int8Value(BigInt("42"))]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("42")));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uint1Value(255)]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uint2Value(65535)]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("255"))]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("255")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Float4Value(3.14)]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new BlobValue(data)]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new DateValue(date)]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await wsClient.admin(
                'MAP {result: $1}',
                [new NoneValue('Int4')]
            );

            expectSingleValueResult(frames, new NoneValue('Int4'));
        }, 1000);

    });

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new BooleanValue(true)]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Int1Value(42)]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Int2Value(1234)]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Int4Value(12345678)]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Int8Value(BigInt("42"))]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("42")));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uint1Value(255)]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uint2Value(65535)]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("255"))]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("255")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Float4Value(3.14)]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new BlobValue(data)]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new DateValue(date)]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await wsClient.command(
                'MAP {result: $1}',
                [new NoneValue('Int4')]
            );

            expectSingleValueResult(frames, new NoneValue('Int4'));
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new BooleanValue(true)]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Int1Value(42)]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Int2Value(1234)]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Int4Value(12345678)]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Int8Value(BigInt("42"))]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("42")));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uint1Value(255)]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uint2Value(65535)]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("255"))]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("255")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Float4Value(3.14)]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new BlobValue(data)]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new DateValue(date)]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)]
            );

            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await wsClient.query(
                'MAP {result: $1}',
                [new NoneValue('Int4')]
            );

            expectSingleValueResult(frames, new NoneValue('Int4'));
        }, 1000);

    });

});
