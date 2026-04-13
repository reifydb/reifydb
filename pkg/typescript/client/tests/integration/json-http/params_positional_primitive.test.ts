// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, JsonHttpClient} from "../../../src";
import {expectSingleResult} from "./test-helper";

describe.each([
    {encoding: "json"},
    {encoding: "rbcf"},
] as const)('Positional Parameters (primitive) [$encoding]', ({encoding}) => {
    let httpClient: JsonHttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_json_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
            encoding,
        });
    });

    describe('admin', () => {

        it('Boolean', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [true]);
            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [42]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [1234]);
            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [12345678]);
            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [BigInt("42")]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [BigInt("170141183460469231731687303715884105727")]);
            expectSingleResult(frames, "170141183460469231731687303715884105727", 'string');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [255]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [65535]);
            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [4294967295]);
            expectSingleResult(frames, 4294967295, 'number');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [BigInt("255")]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [BigInt("340282366920938463463374607431768211455")]);
            expectSingleResult(frames, "340282366920938463463374607431768211455", 'string');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [3.14]);
            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', [3.141592653589793]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.admin('MAP {result: $1}', [decimal]);
            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.admin('MAP {result: $1}', ["Hello, World!"]);
            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.admin('MAP {result: $1}', [data]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.admin('MAP {result: $1}', [date]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.admin('MAP {result: $1}', [time]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.admin('MAP {result: $1}', [datetime]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.admin('MAP {result: $1}', [duration]);
            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.admin('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.admin('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.admin('MAP {result: $1}', [identityId]);
            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

    describe('command', () => {

        it('Boolean', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [true]);
            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [42]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [1234]);
            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [12345678]);
            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [BigInt("42")]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [BigInt("170141183460469231731687303715884105727")]);
            expectSingleResult(frames, "170141183460469231731687303715884105727", 'string');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [255]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [65535]);
            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [4294967295]);
            expectSingleResult(frames, 4294967295, 'number');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [BigInt("255")]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [BigInt("340282366920938463463374607431768211455")]);
            expectSingleResult(frames, "340282366920938463463374607431768211455", 'string');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [3.14]);
            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.command('MAP {result: $1}', [3.141592653589793]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.command('MAP {result: $1}', [decimal]);
            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.command('MAP {result: $1}', ["Hello, World!"]);
            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.command('MAP {result: $1}', [data]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.command('MAP {result: $1}', [date]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.command('MAP {result: $1}', [time]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.command('MAP {result: $1}', [datetime]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.command('MAP {result: $1}', [duration]);
            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.command('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.command('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.command('MAP {result: $1}', [identityId]);
            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

    describe('query', () => {

        it('Boolean', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [true]);
            expectSingleResult(frames, true, 'boolean');
        }, 1000);

        it('Int1', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [42]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int2', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [1234]);
            expectSingleResult(frames, 1234, 'number');
        }, 1000);

        it('Int4', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [12345678]);
            expectSingleResult(frames, 12345678, 'number');
        }, 1000);

        it('Int8', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [BigInt("42")]);
            expectSingleResult(frames, 42, 'number');
        }, 1000);

        it('Int16', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [BigInt("170141183460469231731687303715884105727")]);
            expectSingleResult(frames, "170141183460469231731687303715884105727", 'string');
        }, 1000);

        it('Uint1', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [255]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint2', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [65535]);
            expectSingleResult(frames, 65535, 'number');
        }, 1000);

        it('Uint4', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [4294967295]);
            expectSingleResult(frames, 4294967295, 'number');
        }, 1000);

        it('Uint8', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [BigInt("255")]);
            expectSingleResult(frames, 255, 'number');
        }, 1000);

        it('Uint16', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [BigInt("340282366920938463463374607431768211455")]);
            expectSingleResult(frames, "340282366920938463463374607431768211455", 'string');
        }, 1000);

        it('Float4', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [3.14]);
            expectSingleResult(frames, 3.14, 'number');
        }, 1000);

        it('Float8', async () => {
            const frames = await httpClient.query('MAP {result: $1}', [3.141592653589793]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 14);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Decimal', async () => {
            const decimal = "123.456789";
            const frames = await httpClient.query('MAP {result: $1}', [decimal]);
            expectSingleResult(frames, decimal, 'string');
        }, 1000);

        it('Utf8', async () => {
            const frames = await httpClient.query('MAP {result: $1}', ["Hello, World!"]);
            expectSingleResult(frames, "Hello, World!", 'string');
        }, 1000);

        it('Blob', async () => {
            const data = new Uint8Array([1, 2, 3, 4, 5]);
            const frames = await httpClient.query('MAP {result: $1}', [data]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Date', async () => {
            const date = new Date('2024-03-15');
            const frames = await httpClient.query('MAP {result: $1}', [date]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Time', async () => {
            const time = new Date('1970-01-01T14:30:00.123Z');
            const frames = await httpClient.query('MAP {result: $1}', [time]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('DateTime', async () => {
            const datetime = new Date('2024-03-15T14:30:00.123Z');
            const frames = await httpClient.query('MAP {result: $1}', [datetime]);
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Duration', async () => {
            const duration = "P1DT2H30M";
            const frames = await httpClient.query('MAP {result: $1}', [duration]);
            expectSingleResult(frames, duration, 'string');
        }, 1000);

        it('Uuid4', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await httpClient.query('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('Uuid7', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await httpClient.query('MAP {result: $1}', [uuid]);
            expectSingleResult(frames, uuid, 'string');
        }, 1000);

        it('IdentityId', async () => {
            const identityId = "018fad5d-f37a-7c94-a716-446655440001";
            const frames = await httpClient.query('MAP {result: $1}', [identityId]);
            expectSingleResult(frames, identityId, 'string');
        }, 1000);

    });

});
