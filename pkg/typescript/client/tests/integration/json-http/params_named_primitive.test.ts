// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, JsonHttpClient} from "../../../src";
import {
    BooleanValue, Int1Value, Int2Value, Int4Value, Int8Value,
    Uint1Value, Uint16Value,
    Float8Value, Utf8Value, BlobValue,
    DateTimeValue, Uuid4Value, Uuid7Value
} from "@reifydb/core";
import {expectSingleValueResult} from "./test-helper";

describe('Named Parameters (primitive)', () => {
    let httpClient: JsonHttpClient;

    beforeAll(async () => {
        httpClient = Client.connectJsonHttp(process.env.REIFYDB_HTTP_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN,
        });
    });

    describe('admin', () => {

        it('Boolean', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: true });
            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 42 });
            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 1234 });
            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 12345678 });
            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: BigInt("42") });
            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: BigInt("170141183460469231731687303715884105727") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 255 });
            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 65535 });
            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 4294967295 });
            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: BigInt("255") });
            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: BigInt("340282366920938463463374607431768211455") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 3.14 });
            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: 3.141592653589793 });
            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.admin('MAP {result: $value}', { value: decimal });
            expectSingleValueResult(frames, new Utf8Value(decimal));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.admin('MAP {result: $value}', { value: "Hello, World!" });
            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.admin('MAP {result: $value}', { value: data });
            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.admin('MAP {result: $value}', { value: date });
            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.admin('MAP {result: $value}', { value: time });
            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.admin('MAP {result: $value}', { value: datetime });
            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.admin('MAP {result: $value}', { value: duration });
            expectSingleValueResult(frames, new Utf8Value(duration));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.admin('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.admin('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.admin('MAP {result: $value}', { value: identityId });
            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: true });
            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 42 });
            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 1234 });
            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 12345678 });
            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: BigInt("42") });
            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: BigInt("170141183460469231731687303715884105727") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 255 });
            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 65535 });
            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 4294967295 });
            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: BigInt("255") });
            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: BigInt("340282366920938463463374607431768211455") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 3.14 });
            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: 3.141592653589793 });
            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.command('MAP {result: $value}', { value: decimal });
            expectSingleValueResult(frames, new Utf8Value(decimal));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.command('MAP {result: $value}', { value: "Hello, World!" });
            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.command('MAP {result: $value}', { value: data });
            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.command('MAP {result: $value}', { value: date });
            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.command('MAP {result: $value}', { value: time });
            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.command('MAP {result: $value}', { value: datetime });
            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.command('MAP {result: $value}', { value: duration });
            expectSingleValueResult(frames, new Utf8Value(duration));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.command('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.command('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.command('MAP {result: $value}', { value: identityId });
            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: true });
            expectSingleValueResult(frames, new BooleanValue(true));
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 42 });
            expectSingleValueResult(frames, new Int1Value(42));
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 1234 });
            expectSingleValueResult(frames, new Int2Value(1234));
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 12345678 });
            expectSingleValueResult(frames, new Int4Value(12345678));
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: BigInt("42") });
            expectSingleValueResult(frames, new Uint1Value(42));
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: BigInt("170141183460469231731687303715884105727") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("170141183460469231731687303715884105727")));
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 255 });
            expectSingleValueResult(frames, new Int2Value(255));
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 65535 });
            expectSingleValueResult(frames, new Int4Value(65535));
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 4294967295 });
            expectSingleValueResult(frames, new Int8Value(BigInt("4294967295")));
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: BigInt("255") });
            expectSingleValueResult(frames, new Uint1Value(255));
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: BigInt("340282366920938463463374607431768211455") });
            expectSingleValueResult(frames, new Uint16Value(BigInt("340282366920938463463374607431768211455")));
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 3.14 });
            expectSingleValueResult(frames, new Float8Value(3.14));
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: 3.141592653589793 });
            expectSingleValueResult(frames, new Float8Value(3.141592653589793));
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.query('MAP {result: $value}', { value: decimal });
            expectSingleValueResult(frames, new Utf8Value(decimal));
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.query('MAP {result: $value}', { value: "Hello, World!" });
            expectSingleValueResult(frames, new Utf8Value("Hello, World!"));
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.query('MAP {result: $value}', { value: data });
            expectSingleValueResult(frames, new BlobValue(data));
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.query('MAP {result: $value}', { value: date });
            expectSingleValueResult(frames, new DateTimeValue(date));
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.query('MAP {result: $value}', { value: time });
            expectSingleValueResult(frames, new DateTimeValue(time));
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.query('MAP {result: $value}', { value: datetime });
            expectSingleValueResult(frames, new DateTimeValue(datetime));
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.query('MAP {result: $value}', { value: duration });
            expectSingleValueResult(frames, new Utf8Value(duration));
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.query('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid4Value(uuid));
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.query('MAP {result: $value}', { value: uuid });
            expectSingleValueResult(frames, new Uuid7Value(uuid));
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.query('MAP {result: $value}', { value: identityId });
            expectSingleValueResult(frames, new Uuid7Value(identityId));
        }, 1000);

    });

});
