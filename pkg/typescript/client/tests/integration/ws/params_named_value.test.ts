/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {Client, WsClient} from "../../../src";
import {waitForDatabase} from "../setup";
import {
    BooleanValue, Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    Float4Value, Float8Value, DecimalValue, Utf8Value, BlobValue,
    DateValue, TimeValue, DateTimeValue, DurationValue,
    Uuid4Value, Uuid7Value, UndefinedValue, IdentityIdValue,
    Schema
} from "@reifydb/core";
import { expectSingleValueResult } from "./test-helper";

describe('Named Parameters', () => {
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
                'MAP $value as result',
                { value: new BooleanValue(true) },
                [Schema.object({result: Schema.booleanValue()})]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Int1Value(42) },
                [Schema.object({result: Schema.int1Value()})]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Int2Value(1234) },
                [Schema.object({result: Schema.int2Value()})]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Int4Value(12345678) },
                [Schema.object({result: Schema.int4Value()})]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Int8Value(BigInt("9223372036854775807")) },
                [Schema.object({result: Schema.int8Value()})]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("9223372036854775807")));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Int16Value(BigInt("170141183460469231731687303715884105727")) },
                [Schema.object({result: Schema.int16Value()})]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uint1Value(255) },
                [Schema.object({result: Schema.uint1Value()})]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uint2Value(65535) },
                [Schema.object({result: Schema.uint2Value()})]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uint4Value(4294967295) },
                [Schema.object({result: Schema.uint4Value()})]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uint8Value(BigInt("18446744073709551615")) },
                [Schema.object({result: Schema.uint8Value()})]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("18446744073709551615")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uint16Value(BigInt("340282366920938463463374607431768211455")) },
                [Schema.object({result: Schema.uint16Value()})]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Float4Value(3.14) },
                [Schema.object({result: Schema.float4Value()})]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Float8Value(3.141592653589793) },
                [Schema.object({result: Schema.float8Value()})]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new DecimalValue("123.456789") },
                [Schema.object({result: Schema.decimalValue()})]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Utf8Value("Hello, World!") },
                [Schema.object({result: Schema.utf8Value()})]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new BlobValue(data) },
                [Schema.object({result: Schema.blobValue()})]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new DateValue(date) },
                [Schema.object({result: Schema.dateValue()})]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new TimeValue("14:30:00.123456789") },
                [Schema.object({result: Schema.timeValue()})]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new DateTimeValue(datetime) },
                [Schema.object({result: Schema.dateTimeValue()})]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new DurationValue("P1DT2H30M") },
                [Schema.object({result: Schema.durationValue()})]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uuid4Value(uuid) },
                [Schema.object({result: Schema.uuid4Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new Uuid7Value(uuid) },
                [Schema.object({result: Schema.uuid7Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new IdentityIdValue(identityId) },
                [Schema.object({result: Schema.identityIdValue()})]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('Undefined', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                { value: new UndefinedValue() },
                [Schema.object({result: Schema.undefinedValue()})]
            );

            expectSingleValueResult(frames, new UndefinedValue());
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new BooleanValue(true) },
                [Schema.object({result: Schema.booleanValue()})]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Int1Value(42) },
                [Schema.object({result: Schema.int1Value()})]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Int2Value(1234) },
                [Schema.object({result: Schema.int2Value()})]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Int4Value(12345678) },
                [Schema.object({result: Schema.int4Value()})]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Int8Value(BigInt("9223372036854775807")) },
                [Schema.object({result: Schema.int8Value()})]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("9223372036854775807")));
        }, 1000);

        it('Int16', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Int16Value(BigInt("170141183460469231731687303715884105727")) },
                [Schema.object({result: Schema.int16Value()})]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uint1Value(255) },
                [Schema.object({result: Schema.uint1Value()})]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uint2Value(65535) },
                [Schema.object({result: Schema.uint2Value()})]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uint4Value(4294967295) },
                [Schema.object({result: Schema.uint4Value()})]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uint8Value(BigInt("18446744073709551615")) },
                [Schema.object({result: Schema.uint8Value()})]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("18446744073709551615")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uint16Value(BigInt("340282366920938463463374607431768211455")) },
                [Schema.object({result: Schema.uint16Value()})]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Float4Value(3.14) },
                [Schema.object({result: Schema.float4Value()})]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Float8Value(3.141592653589793) },
                [Schema.object({result: Schema.float8Value()})]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new DecimalValue("123.456789") },
                [Schema.object({result: Schema.decimalValue()})]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Utf8Value("Hello, World!") },
                [Schema.object({result: Schema.utf8Value()})]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new BlobValue(data) },
                [Schema.object({result: Schema.blobValue()})]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new DateValue(date) },
                [Schema.object({result: Schema.dateValue()})]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new TimeValue("14:30:00.123456789") },
                [Schema.object({result: Schema.timeValue()})]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new DateTimeValue(datetime) },
                [Schema.object({result: Schema.dateTimeValue()})]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new DurationValue("P1DT2H30M") },
                [Schema.object({result: Schema.durationValue()})]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uuid4Value(uuid) },
                [Schema.object({result: Schema.uuid4Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new Uuid7Value(uuid) },
                [Schema.object({result: Schema.uuid7Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new IdentityIdValue(identityId) },
                [Schema.object({result: Schema.identityIdValue()})]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('Undefined', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                { value: new UndefinedValue() },
                [Schema.object({result: Schema.undefinedValue()})]
            );

            expectSingleValueResult(frames, new UndefinedValue());
        }, 1000);

    });

});