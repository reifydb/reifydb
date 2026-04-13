// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, HttpClient} from "../../../src";
import {
    BooleanValue, Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    Float4Value, Float8Value, DecimalValue, Utf8Value, BlobValue,
    DateValue, TimeValue, DateTimeValue, DurationValue,
    Uuid4Value, Uuid7Value, NoneValue, IdentityIdValue,
    Shape
} from "@reifydb/core";
import { expectSingleValueResult } from "./test-helper";

describe.each([
    {encoding: "json"},
    {encoding: "rbcf"},
] as const)('Positional Parameters (value) [$encoding]', ({encoding}) => {
    let httpClient: HttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
            encoding,
        });
    });

    describe('admin', () => {
        it('Boolean', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new BooleanValue(true)],
                [Shape.object({result: Shape.booleanValue()})]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Int1Value(42)],
                [Shape.object({result: Shape.int1Value()})]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Int2Value(1234)],
                [Shape.object({result: Shape.int2Value()})]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Int4Value(12345678)],
                [Shape.object({result: Shape.int4Value()})]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Int8Value(BigInt("9223372036854775807"))],
                [Shape.object({result: Shape.int8Value()})]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("9223372036854775807")));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))],
                [Shape.object({result: Shape.int16Value()})]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uint1Value(255)],
                [Shape.object({result: Shape.uint1Value()})]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uint2Value(65535)],
                [Shape.object({result: Shape.uint2Value()})]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)],
                [Shape.object({result: Shape.uint4Value()})]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("18446744073709551615"))],
                [Shape.object({result: Shape.uint8Value()})]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("18446744073709551615")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))],
                [Shape.object({result: Shape.uint16Value()})]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Float4Value(3.14)],
                [Shape.object({result: Shape.float4Value()})]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)],
                [Shape.object({result: Shape.float8Value()})]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")],
                [Shape.object({result: Shape.decimalValue()})]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")],
                [Shape.object({result: Shape.utf8Value()})]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new BlobValue(data)],
                [Shape.object({result: Shape.blobValue()})]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new DateValue(date)],
                [Shape.object({result: Shape.dateValue()})]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")],
                [Shape.object({result: Shape.timeValue()})]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)],
                [Shape.object({result: Shape.dateTimeValue()})]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")],
                [Shape.object({result: Shape.durationValue()})]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)],
                [Shape.object({result: Shape.uuid4Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)],
                [Shape.object({result: Shape.uuid7Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)],
                [Shape.object({result: Shape.identityIdValue()})]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $1}',
                [new NoneValue()],
                [Shape.object({result: Shape.noneValue()})]
            );

            expectSingleValueResult(frames, new NoneValue());
        }, 1000);

    });

    describe('command', () => {
        it('Boolean', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new BooleanValue(true)],
                [Shape.object({result: Shape.booleanValue()})]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Int1Value(42)],
                [Shape.object({result: Shape.int1Value()})]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Int2Value(1234)],
                [Shape.object({result: Shape.int2Value()})]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Int4Value(12345678)],
                [Shape.object({result: Shape.int4Value()})]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Int8Value(BigInt("9223372036854775807"))],
                [Shape.object({result: Shape.int8Value()})]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("9223372036854775807")));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))],
                [Shape.object({result: Shape.int16Value()})]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uint1Value(255)],
                [Shape.object({result: Shape.uint1Value()})]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uint2Value(65535)],
                [Shape.object({result: Shape.uint2Value()})]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)],
                [Shape.object({result: Shape.uint4Value()})]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("18446744073709551615"))],
                [Shape.object({result: Shape.uint8Value()})]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("18446744073709551615")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))],
                [Shape.object({result: Shape.uint16Value()})]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Float4Value(3.14)],
                [Shape.object({result: Shape.float4Value()})]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)],
                [Shape.object({result: Shape.float8Value()})]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")],
                [Shape.object({result: Shape.decimalValue()})]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")],
                [Shape.object({result: Shape.utf8Value()})]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new BlobValue(data)],
                [Shape.object({result: Shape.blobValue()})]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new DateValue(date)],
                [Shape.object({result: Shape.dateValue()})]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")],
                [Shape.object({result: Shape.timeValue()})]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)],
                [Shape.object({result: Shape.dateTimeValue()})]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")],
                [Shape.object({result: Shape.durationValue()})]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)],
                [Shape.object({result: Shape.uuid4Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)],
                [Shape.object({result: Shape.uuid7Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)],
                [Shape.object({result: Shape.identityIdValue()})]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await httpClient.command(
                'MAP {result: $1}',
                [new NoneValue()],
                [Shape.object({result: Shape.noneValue()})]
            );

            expectSingleValueResult(frames, new NoneValue());
        }, 1000);

    });

    describe('query', () => {
        it('Boolean', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new BooleanValue(true)],
                [Shape.object({result: Shape.booleanValue()})]
            );

            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Int1Value(42)],
                [Shape.object({result: Shape.int1Value()})]
            );

            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Int2Value(1234)],
                [Shape.object({result: Shape.int2Value()})]
            );

            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Int4Value(12345678)],
                [Shape.object({result: Shape.int4Value()})]
            );

            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Int8Value(BigInt("9223372036854775807"))],
                [Shape.object({result: Shape.int8Value()})]
            );

            expectSingleValueResult(frames, new Int8Value(BigInt("9223372036854775807")));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Int16Value(BigInt("170141183460469231731687303715884105727"))],
                [Shape.object({result: Shape.int16Value()})]
            );

            expectSingleValueResult(frames, new Int16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uint1Value(255)],
                [Shape.object({result: Shape.uint1Value()})]
            );

            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uint2Value(65535)],
                [Shape.object({result: Shape.uint2Value()})]
            );

            expectSingleValueResult(frames, new Uint2Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uint4Value(4294967295)],
                [Shape.object({result: Shape.uint4Value()})]
            );

            expectSingleValueResult(frames, new Uint4Value(4294967295));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uint8Value(BigInt("18446744073709551615"))],
                [Shape.object({result: Shape.uint8Value()})]
            );

            expectSingleValueResult(frames, new Uint8Value(BigInt("18446744073709551615")));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uint16Value(BigInt("340282366920938463463374607431768211455"))],
                [Shape.object({result: Shape.uint16Value()})]
            );

            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Float4Value(3.14)],
                [Shape.object({result: Shape.float4Value()})]
            );

            expectSingleValueResult(frames, new Float4Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Float8Value(3.141592653589793)],
                [Shape.object({result: Shape.float8Value()})]
            );

            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new DecimalValue("123.456789")],
                [Shape.object({result: Shape.decimalValue()})]
            );

            expectSingleValueResult(frames, new DecimalValue("123.456789"));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Utf8Value("Hello, World!")],
                [Shape.object({result: Shape.utf8Value()})]
            );

            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new BlobValue(data)],
                [Shape.object({result: Shape.blobValue()})]
            );

            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new DateValue(date)],
                [Shape.object({result: Shape.dateValue()})]
            );

            expectSingleValueResult(frames, new DateValue(date));
        }, 1000);

        it('Time', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new TimeValue("14:30:00.123456789")],
                [Shape.object({result: Shape.timeValue()})]
            );

            expectSingleValueResult(frames, new TimeValue("14:30:00.123456789"));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new DateTimeValue(datetime)],
                [Shape.object({result: Shape.dateTimeValue()})]
            );

            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new DurationValue("P1DT2H30M")],
                [Shape.object({result: Shape.durationValue()})]
            );

            expectSingleValueResult(frames, new DurationValue("P1DT2H30M"));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uuid4Value(uuid)],
                [Shape.object({result: Shape.uuid4Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new Uuid7Value(uuid)],
                [Shape.object({result: Shape.uuid7Value()})]
            );

            expectSingleValueResult(frames, uuid.includes("550e8400") ? new Uuid4Value(uuid) : new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new IdentityIdValue(identityId)],
                [Shape.object({result: Shape.identityIdValue()})]
            );

            expectSingleValueResult(frames, new IdentityIdValue(identityId));
        }, 1000);

        it('None', async () => {
            const frames = await httpClient.query(
                'MAP {result: $1}',
                [new NoneValue()],
                [Shape.object({result: Shape.noneValue()})]
            );

            expectSingleValueResult(frames, new NoneValue());
        }, 1000);

    });

});
