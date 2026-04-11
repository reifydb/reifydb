// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, HttpClient} from "../../../src";
import {Shape} from "@reifydb/core";
import {
    expectSingleResult,
    expectSingleDateResult,
    expectSingleBlobResult,
    expectSingleBigIntResult
} from "./test-helper";

describe('Named Parameters', () => {
    let httpClient: HttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    });

    describe('admin', () => {

        it('Boolean', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: true },
                [Shape.object({result: Shape.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 42 },
                [Shape.object({result: Shape.int1()})]
            );

            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 1234 },
                [Shape.object({result: Shape.int2()})]
            );

            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 12345678 },
                [Shape.object({result: Shape.int4()})]
            );

            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: BigInt("9223372036854775807") },
                [Shape.object({result: Shape.int8()})]
            );

            expectSingleResult(frames, BigInt("9223372036854775807"), 'bigint');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: BigInt("170141183460469231731687303715884105727") },
                [Shape.object({result: Shape.int16()})]
            );

            expectSingleResult(frames, BigInt("170141183460469231731687303715884105727"), 'bigint');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 255 },
                [Shape.object({result: Shape.uint1()})]
            );

            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 65535 },
                [Shape.object({result: Shape.uint2()})]
            );

            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 4294967295 },
                [Shape.object({result: Shape.uint4()})]
            );

            expectSingleResult(frames, BigInt(4294967295), 'bigint');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: BigInt("18446744073709551615") },
                [Shape.object({result: Shape.uint8()})]
            );

            expectSingleResult(frames, BigInt("18446744073709551615"), 'bigint');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: BigInt("340282366920938463463374607431768211455") },
                [Shape.object({result: Shape.uint16()})]
            );

            expectSingleResult(frames, BigInt("340282366920938463463374607431768211455"), 'bigint');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 3.14 },
                [Shape.object({result: Shape.float4()})]
            );

            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: 3.141592653589793 },
                [Shape.object({result: Shape.float8()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: decimal },
                [Shape.object({result: Shape.decimal()})]
            );

            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: "Hello, World!" },
                [Shape.object({result: Shape.utf8()})]
            );

            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: data },
                [Shape.object({result: Shape.blob()})]
            );

            expectSingleBlobResult(frames, data);
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: date },
                [Shape.object({result: Shape.date()})]
            );

            expectSingleDateResult(frames, date);
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: time },
                [Shape.object({result: Shape.time()})]
            );

            expectSingleDateResult(frames, time);
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: datetime },
                [Shape.object({result: Shape.datetime()})]
            );

            expectSingleDateResult(frames, datetime);
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: duration },
                [Shape.object({result: Shape.duration()})]
            );

            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid4()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid7()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.admin(
                'MAP {result: $value}',
                { value: identityId },
                [Shape.object({result: Shape.identityid()})]
            );

            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: true },
                [Shape.object({result: Shape.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 42 },
                [Shape.object({result: Shape.int1()})]
            );

            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 1234 },
                [Shape.object({result: Shape.int2()})]
            );

            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 12345678 },
                [Shape.object({result: Shape.int4()})]
            );

            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: BigInt("9223372036854775807") },
                [Shape.object({result: Shape.int8()})]
            );

            expectSingleResult(frames, BigInt("9223372036854775807"), 'bigint');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: BigInt("170141183460469231731687303715884105727") },
                [Shape.object({result: Shape.int16()})]
            );

            expectSingleResult(frames, BigInt("170141183460469231731687303715884105727"), 'bigint');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 255 },
                [Shape.object({result: Shape.uint1()})]
            );

            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 65535 },
                [Shape.object({result: Shape.uint2()})]
            );

            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 4294967295 },
                [Shape.object({result: Shape.uint4()})]
            );

            expectSingleResult(frames, BigInt(4294967295), 'bigint');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: BigInt("18446744073709551615") },
                [Shape.object({result: Shape.uint8()})]
            );

            expectSingleResult(frames, BigInt("18446744073709551615"), 'bigint');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: BigInt("340282366920938463463374607431768211455") },
                [Shape.object({result: Shape.uint16()})]
            );

            expectSingleResult(frames, BigInt("340282366920938463463374607431768211455"), 'bigint');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 3.14 },
                [Shape.object({result: Shape.float4()})]
            );

            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: 3.141592653589793 },
                [Shape.object({result: Shape.float8()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: decimal },
                [Shape.object({result: Shape.decimal()})]
            );

            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: "Hello, World!" },
                [Shape.object({result: Shape.utf8()})]
            );

            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: data },
                [Shape.object({result: Shape.blob()})]
            );

            expectSingleBlobResult(frames, data);
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: date },
                [Shape.object({result: Shape.date()})]
            );

            expectSingleDateResult(frames, date);
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: time },
                [Shape.object({result: Shape.time()})]
            );

            expectSingleDateResult(frames, time);
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: datetime },
                [Shape.object({result: Shape.datetime()})]
            );

            expectSingleDateResult(frames, datetime);
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: duration },
                [Shape.object({result: Shape.duration()})]
            );

            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid4()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid7()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.command(
                'MAP {result: $value}',
                { value: identityId },
                [Shape.object({result: Shape.identityid()})]
            );

            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: true },
                [Shape.object({result: Shape.boolean()})]
            );

            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 42 },
                [Shape.object({result: Shape.int1()})]
            );

            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 1234 },
                [Shape.object({result: Shape.int2()})]
            );

            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 12345678 },
                [Shape.object({result: Shape.int4()})]
            );

            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: BigInt("9223372036854775807") },
                [Shape.object({result: Shape.int8()})]
            );

            expectSingleResult(frames, BigInt("9223372036854775807"), 'bigint');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: BigInt("170141183460469231731687303715884105727") },
                [Shape.object({result: Shape.int16()})]
            );

            expectSingleResult(frames, BigInt("170141183460469231731687303715884105727"), 'bigint');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 255 },
                [Shape.object({result: Shape.uint1()})]
            );

            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 65535 },
                [Shape.object({result: Shape.uint2()})]
            );

            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 4294967295 },
                [Shape.object({result: Shape.uint4()})]
            );

            expectSingleResult(frames, BigInt(4294967295), 'bigint');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: BigInt("18446744073709551615") },
                [Shape.object({result: Shape.uint8()})]
            );

            expectSingleResult(frames, BigInt("18446744073709551615"), 'bigint');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: BigInt("340282366920938463463374607431768211455") },
                [Shape.object({result: Shape.uint16()})]
            );

            expectSingleResult(frames, BigInt("340282366920938463463374607431768211455"), 'bigint');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 3.14 },
                [Shape.object({result: Shape.float4()})]
            );

            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: 3.141592653589793 },
                [Shape.object({result: Shape.float8()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: decimal },
                [Shape.object({result: Shape.decimal()})]
            );

            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: "Hello, World!" },
                [Shape.object({result: Shape.utf8()})]
            );

            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: data },
                [Shape.object({result: Shape.blob()})]
            );

            expectSingleBlobResult(frames, data);
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: date },
                [Shape.object({result: Shape.date()})]
            );

            expectSingleDateResult(frames, date);
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: time },
                [Shape.object({result: Shape.time()})]
            );

            expectSingleDateResult(frames, time);
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: datetime },
                [Shape.object({result: Shape.datetime()})]
            );

            expectSingleDateResult(frames, datetime);
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: duration },
                [Shape.object({result: Shape.duration()})]
            );

            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid4()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: uuid },
                [Shape.object({result: Shape.uuid7()})]
            );

            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.query(
                'MAP {result: $value}',
                { value: identityId },
                [Shape.object({result: Shape.identityid()})]
            );

            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

});
