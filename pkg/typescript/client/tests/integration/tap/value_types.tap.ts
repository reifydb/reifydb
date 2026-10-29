// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
// Generated from: value_types.tap.md

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Option, Shape} from "@reifydb/core";

describe('Value Types', () => {
    let client: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        client = await Client.connectWs(process.env.REIFYDB_WS_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    }, 15000);

    afterEach(async () => {
        if (client) {
            client.disconnect();
            client = null;
        }
    });

    it('bool_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_bool_values CASCADE; CREATE NAMESPACE vt_ws_bool_values; CREATE TABLE vt_ws_bool_values::t { id: int4, v: bool }; INSERT vt_ws_bool_values::t [{ id: 1, v: true }, { id: 2, v: false }]; OUTPUT FROM vt_ws_bool_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.boolean()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: true}, {'#rownum': 2, id: 2, v: false}]);
    });

    it('int1_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_int1_bounds CASCADE; CREATE NAMESPACE vt_ws_int1_bounds; CREATE TABLE vt_ws_int1_bounds::t { id: int4, v: int1 }; INSERT vt_ws_int1_bounds::t [{ id: 1, v: -128 }, { id: 2, v: 127 }]; OUTPUT FROM vt_ws_int1_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.int1()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: -128}, {'#rownum': 2, id: 2, v: 127}]);
    });

    it('int2_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_int2_bounds CASCADE; CREATE NAMESPACE vt_ws_int2_bounds; CREATE TABLE vt_ws_int2_bounds::t { id: int4, v: int2 }; INSERT vt_ws_int2_bounds::t [{ id: 1, v: -32768 }, { id: 2, v: 32767 }]; OUTPUT FROM vt_ws_int2_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.int2()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: -32768}, {'#rownum': 2, id: 2, v: 32767}]);
    });

    it('int4_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_int4_bounds CASCADE; CREATE NAMESPACE vt_ws_int4_bounds; CREATE TABLE vt_ws_int4_bounds::t { id: int4, v: int4 }; INSERT vt_ws_int4_bounds::t [{ id: 1, v: -2147483648 }, { id: 2, v: 2147483647 }]; OUTPUT FROM vt_ws_int4_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.int4()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: -2147483648}, {'#rownum': 2, id: 2, v: 2147483647}]);
    });

    it('int8_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_int8_bounds CASCADE; CREATE NAMESPACE vt_ws_int8_bounds; CREATE TABLE vt_ws_int8_bounds::t { id: int4, v: int8 }; INSERT vt_ws_int8_bounds::t [{ id: 1, v: -9223372036854775808 }, { id: 2, v: 9223372036854775807 }]; OUTPUT FROM vt_ws_int8_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.int8()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: -9223372036854775808n}, {'#rownum': 2, id: 2, v: 9223372036854775807n}]);
    });

    it('int16_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_int16_bounds CASCADE; CREATE NAMESPACE vt_ws_int16_bounds; CREATE TABLE vt_ws_int16_bounds::t { id: int4, v: int16 }; INSERT vt_ws_int16_bounds::t [{ id: 1, v: -170141183460469231731687303715884105728 }, { id: 2, v: 170141183460469231731687303715884105727 }]; OUTPUT FROM vt_ws_int16_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.int16()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: -170141183460469231731687303715884105728n}, {'#rownum': 2, id: 2, v: 170141183460469231731687303715884105727n}]);
    });

    it('uint1_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uint1_bounds CASCADE; CREATE NAMESPACE vt_ws_uint1_bounds; CREATE TABLE vt_ws_uint1_bounds::t { id: int4, v: uint1 }; INSERT vt_ws_uint1_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 255 }]; OUTPUT FROM vt_ws_uint1_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uint1()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0}, {'#rownum': 2, id: 2, v: 255}]);
    });

    it('uint2_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uint2_bounds CASCADE; CREATE NAMESPACE vt_ws_uint2_bounds; CREATE TABLE vt_ws_uint2_bounds::t { id: int4, v: uint2 }; INSERT vt_ws_uint2_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 65535 }]; OUTPUT FROM vt_ws_uint2_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uint2()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0}, {'#rownum': 2, id: 2, v: 65535}]);
    });

    it('uint4_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uint4_bounds CASCADE; CREATE NAMESPACE vt_ws_uint4_bounds; CREATE TABLE vt_ws_uint4_bounds::t { id: int4, v: uint4 }; INSERT vt_ws_uint4_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 4294967295 }]; OUTPUT FROM vt_ws_uint4_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uint4()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0}, {'#rownum': 2, id: 2, v: 4294967295}]);
    });

    it('uint8_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uint8_bounds CASCADE; CREATE NAMESPACE vt_ws_uint8_bounds; CREATE TABLE vt_ws_uint8_bounds::t { id: int4, v: uint8 }; INSERT vt_ws_uint8_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 18446744073709551615 }]; OUTPUT FROM vt_ws_uint8_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uint8()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0n}, {'#rownum': 2, id: 2, v: 18446744073709551615n}]);
    });

    it('uint16_bounds', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uint16_bounds CASCADE; CREATE NAMESPACE vt_ws_uint16_bounds; CREATE TABLE vt_ws_uint16_bounds::t { id: int4, v: uint16 }; INSERT vt_ws_uint16_bounds::t [{ id: 1, v: 0 }, { id: 2, v: 340282366920938463463374607431768211455 }]; OUTPUT FROM vt_ws_uint16_bounds::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uint16()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0n}, {'#rownum': 2, id: 2, v: 340282366920938463463374607431768211455n}]);
    });

    it('float4_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_float4_values CASCADE; CREATE NAMESPACE vt_ws_float4_values; CREATE TABLE vt_ws_float4_values::t { id: int4, v: float4 }; INSERT vt_ws_float4_values::t [{ id: 1, v: 1.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000001 }, { id: 4, v: 300000000000000000000000000000000000000.0 }]; OUTPUT FROM vt_ws_float4_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.float4()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 1.5}, {'#rownum': 2, id: 2, v: 0}, {'#rownum': 3, id: 3, v: Math.fround(0.000001)}, {'#rownum': 4, id: 4, v: Math.fround(3e38)}]);
    });

    it('float8_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_float8_values CASCADE; CREATE NAMESPACE vt_ws_float8_values; CREATE TABLE vt_ws_float8_values::t { id: int4, v: float8 }; INSERT vt_ws_float8_values::t [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000000000001 }, { id: 4, v: 1000000000000000000000.0 }]; OUTPUT FROM vt_ws_float8_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.float8()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: 0.5}, {'#rownum': 2, id: 2, v: 0}, {'#rownum': 3, id: 3, v: 1e-12}, {'#rownum': 4, id: 4, v: 1e21}]);
    });

    it('decimal_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_decimal_values CASCADE; CREATE NAMESPACE vt_ws_decimal_values; CREATE TABLE vt_ws_decimal_values::t { id: int4, v: decimal }; INSERT vt_ws_decimal_values::t [{ id: 1, v: '12345678901234567890.123456789' }, { id: 2, v: '1.500' }, { id: 3, v: '-0.25' }]; OUTPUT FROM vt_ws_decimal_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.decimal()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '12345678901234567890.1234567890'}, {'#rownum': 2, id: 2, v: '1.5000000000'}, {'#rownum': 3, id: 3, v: '-0.2500000000'}]);
    });

    it('decimal_declared_scale', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_decimal_declared_scale CASCADE; CREATE NAMESPACE vt_ws_decimal_declared_scale; CREATE TABLE vt_ws_decimal_declared_scale::t { id: int4, v: decimal(30, 3) }; INSERT vt_ws_decimal_declared_scale::t [{ id: 1, v: '12345678901234567890.123' }, { id: 2, v: '1.500' }, { id: 3, v: '-0.25' }]; OUTPUT FROM vt_ws_decimal_declared_scale::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.decimal()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '12345678901234567890.123'}, {'#rownum': 2, id: 2, v: '1.500'}, {'#rownum': 3, id: 3, v: '-0.250'}]);
    });

    it('utf8_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_utf8_values CASCADE; CREATE NAMESPACE vt_ws_utf8_values; CREATE TABLE vt_ws_utf8_values::t { id: int4, v: utf8 }; INSERT vt_ws_utf8_values::t [{ id: 1, v: '' }, { id: 2, v: 'héllo wörld' }, { id: 3, v: '日本語' }]; OUTPUT FROM vt_ws_utf8_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.utf8()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: ''}, {'#rownum': 2, id: 2, v: 'héllo wörld'}, {'#rownum': 3, id: 3, v: '日本語'}]);
    });

    it('blob_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_blob_values CASCADE; CREATE NAMESPACE vt_ws_blob_values; CREATE TABLE vt_ws_blob_values::t { id: int4, v: blob }; INSERT vt_ws_blob_values::t [{ id: 1, v: blob::hex('') }, { id: 2, v: blob::hex('00ff10') }]; OUTPUT FROM vt_ws_blob_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.blob()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: new Uint8Array([])}, {'#rownum': 2, id: 2, v: new Uint8Array([0x00, 0xff, 0x10])}]);
    });

    it('date_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_date_values CASCADE; CREATE NAMESPACE vt_ws_date_values; CREATE TABLE vt_ws_date_values::t { id: int4, v: date }; INSERT vt_ws_date_values::t [{ id: 1, v: '2024-02-29' }, { id: 2, v: '1970-01-01' }]; OUTPUT FROM vt_ws_date_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.date()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: new Date('2024-02-29T00:00:00.000Z')}, {'#rownum': 2, id: 2, v: new Date('1970-01-01T00:00:00.000Z')}]);
    });

    it('time_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_time_values CASCADE; CREATE NAMESPACE vt_ws_time_values; CREATE TABLE vt_ws_time_values::t { id: int4, v: time }; INSERT vt_ws_time_values::t [{ id: 1, v: '00:00:00' }, { id: 2, v: '23:59:59.999999999' }]; OUTPUT FROM vt_ws_time_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.time()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '00:00:00.000000000'}, {'#rownum': 2, id: 2, v: '23:59:59.999999999'}]);
    });

    it('datetime_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_datetime_values CASCADE; CREATE NAMESPACE vt_ws_datetime_values; CREATE TABLE vt_ws_datetime_values::t { id: int4, v: datetime }; INSERT vt_ws_datetime_values::t [{ id: 1, v: '2024-02-29T12:34:56.789Z' }, { id: 2, v: '1970-01-01T00:00:00Z' }]; OUTPUT FROM vt_ws_datetime_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.datetime()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: new Date('2024-02-29T12:34:56.789Z')}, {'#rownum': 2, id: 2, v: new Date('1970-01-01T00:00:00.000Z')}]);
    });

    it('duration_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_duration_values CASCADE; CREATE NAMESPACE vt_ws_duration_values; CREATE TABLE vt_ws_duration_values::t { id: int4, v: duration }; INSERT vt_ws_duration_values::t [{ id: 1, v: '1d2h30m' }, { id: 2, v: '0s' }]; OUTPUT FROM vt_ws_duration_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.duration()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '1d2h30m'}, {'#rownum': 2, id: 2, v: '0s'}]);
    });

    it('uuid4_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uuid4_values CASCADE; CREATE NAMESPACE vt_ws_uuid4_values; CREATE TABLE vt_ws_uuid4_values::t { id: int4, v: uuid4 }; INSERT vt_ws_uuid4_values::t [{ id: 1, v: '550e8400-e29b-41d4-a716-446655440000' }]; OUTPUT FROM vt_ws_uuid4_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uuid4()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '550e8400-e29b-41d4-a716-446655440000'}]);
    });

    it('uuid7_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_uuid7_values CASCADE; CREATE NAMESPACE vt_ws_uuid7_values; CREATE TABLE vt_ws_uuid7_values::t { id: int4, v: uuid7 }; INSERT vt_ws_uuid7_values::t [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }]; OUTPUT FROM vt_ws_uuid7_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.uuid7()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '018fad5d-f37a-7c94-a716-446655440000'}]);
    });

    it('identity_id_values', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_identity_id_values CASCADE; CREATE NAMESPACE vt_ws_identity_id_values; CREATE TABLE vt_ws_identity_id_values::t { id: int4, v: identity_id }; INSERT vt_ws_identity_id_values::t [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440001' }]; OUTPUT FROM vt_ws_identity_id_values::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.identityid()})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: '018fad5d-f37a-7c94-a716-446655440001'}]);
    });

    it('option_int4_none', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_option_int4_none CASCADE; CREATE NAMESPACE vt_ws_option_int4_none; CREATE TABLE vt_ws_option_int4_none::t { id: int4, v: Option(int4) }; INSERT vt_ws_option_int4_none::t [{ id: 1, v: 7 }, { id: 2, v: none }]; OUTPUT FROM vt_ws_option_int4_none::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(7)}, {'#rownum': 2, id: 2, v: Option.none('Int4')}]);
    });

    it('option_utf8_none', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_option_utf8_none CASCADE; CREATE NAMESPACE vt_ws_option_utf8_none; CREATE TABLE vt_ws_option_utf8_none::t { id: int4, v: Option(utf8) }; INSERT vt_ws_option_utf8_none::t [{ id: 1, v: none }, { id: 2, v: 'x' }]; OUTPUT FROM vt_ws_option_utf8_none::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, v: Option.none('Utf8')}, {'#rownum': 2, id: 2, v: Option.some('x')}]);
    });

    it('all_types', async () => {
        const frames = await client.admin(
            "DROP NAMESPACE IF EXISTS vt_ws_all_types CASCADE; CREATE NAMESPACE vt_ws_all_types; CREATE TABLE vt_ws_all_types::t { id: int4, cbool: bool, cint1: int1, cint2: int2, cint4: int4, cint8: int8, cint16: int16, cuint1: uint1, cuint2: uint2, cuint4: uint4, cuint8: uint8, cuint16: uint16, cfloat4: float4, cfloat8: float8, cdecimal: decimal, cdecimalscaled: decimal(30, 3), cutf8: utf8, cblob: blob, cdate: date, ctime: time, cdatetime: datetime, cduration: duration, cuuid4: uuid4, cuuid7: uuid7, cidentityid: identity_id, coptint4: Option(int4), coptutf8: Option(utf8) }; INSERT vt_ws_all_types::t [{ id: 1, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808, cint16: -170141183460469231731687303715884105728, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0, cuint16: 0, cfloat4: 1.5, cfloat8: 0.5, cdecimal: '12345678901234567890.123456789', cdecimalscaled: '12345678901234567890.123', cutf8: '', cblob: blob::hex(''), cdate: '2024-02-29', ctime: '00:00:00', cdatetime: '2024-02-29T12:34:56.789Z', cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: 7, coptutf8: none }, { id: 2, cbool: false, cint1: 127, cint2: 32767, cint4: 2147483647, cint8: 9223372036854775807, cint16: 170141183460469231731687303715884105727, cuint1: 255, cuint2: 65535, cuint4: 4294967295, cuint8: 18446744073709551615, cuint16: 340282366920938463463374607431768211455, cfloat4: -0.0, cfloat8: -0.0, cdecimal: '1.500', cdecimalscaled: '1.500', cutf8: 'héllo wörld', cblob: blob::hex('00ff10'), cdate: '1970-01-01', ctime: '23:59:59.999999999', cdatetime: '1970-01-01T00:00:00Z', cduration: '0s', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: none, coptutf8: 'x' }, { id: 3, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808, cint16: -170141183460469231731687303715884105728, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0, cuint16: 0, cfloat4: 0.000001, cfloat8: 0.000000000001, cdecimal: '-0.25', cdecimalscaled: '-0.25', cutf8: '日本語', cblob: blob::hex(''), cdate: '2024-02-29', ctime: '00:00:00', cdatetime: '2024-02-29T12:34:56.789Z', cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: 7, coptutf8: none }]; OUTPUT FROM vt_ws_all_types::t SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), cbool: Shape.boolean(), cint1: Shape.int1(), cint2: Shape.int2(), cint4: Shape.int4(), cint8: Shape.int8(), cint16: Shape.int16(), cuint1: Shape.uint1(), cuint2: Shape.uint2(), cuint4: Shape.uint4(), cuint8: Shape.uint8(), cuint16: Shape.uint16(), cfloat4: Shape.float4(), cfloat8: Shape.float8(), cdecimal: Shape.decimal(), cdecimalscaled: Shape.decimal(), cutf8: Shape.utf8(), cblob: Shape.blob(), cdate: Shape.date(), ctime: Shape.time(), cdatetime: Shape.datetime(), cduration: Shape.duration(), cuuid4: Shape.uuid4(), cuuid7: Shape.uuid7(), cidentityid: Shape.identityid(), coptint4: Shape.option(Shape.int4()), coptutf8: Shape.option(Shape.utf8())})]
        );
        expect(frames).toHaveLength(1);
        expect(frames[0]).toEqual([{'#rownum': 1, id: 1, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808n, cint16: -170141183460469231731687303715884105728n, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0n, cuint16: 0n, cfloat4: 1.5, cfloat8: 0.5, cdecimal: '12345678901234567890.1234567890', cdecimalscaled: '12345678901234567890.123', cutf8: '', cblob: new Uint8Array([]), cdate: new Date('2024-02-29T00:00:00.000Z'), ctime: '00:00:00.000000000', cdatetime: new Date('2024-02-29T12:34:56.789Z'), cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: Option.some(7), coptutf8: Option.none('Utf8')}, {'#rownum': 2, id: 2, cbool: false, cint1: 127, cint2: 32767, cint4: 2147483647, cint8: 9223372036854775807n, cint16: 170141183460469231731687303715884105727n, cuint1: 255, cuint2: 65535, cuint4: 4294967295, cuint8: 18446744073709551615n, cuint16: 340282366920938463463374607431768211455n, cfloat4: 0, cfloat8: 0, cdecimal: '1.5000000000', cdecimalscaled: '1.500', cutf8: 'héllo wörld', cblob: new Uint8Array([0x00, 0xff, 0x10]), cdate: new Date('1970-01-01T00:00:00.000Z'), ctime: '23:59:59.999999999', cdatetime: new Date('1970-01-01T00:00:00.000Z'), cduration: '0s', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: Option.none('Int4'), coptutf8: Option.some('x')}, {'#rownum': 3, id: 3, cbool: true, cint1: -128, cint2: -32768, cint4: -2147483648, cint8: -9223372036854775808n, cint16: -170141183460469231731687303715884105728n, cuint1: 0, cuint2: 0, cuint4: 0, cuint8: 0n, cuint16: 0n, cfloat4: Math.fround(0.000001), cfloat8: 1e-12, cdecimal: '-0.2500000000', cdecimalscaled: '-0.250', cutf8: '日本語', cblob: new Uint8Array([]), cdate: new Date('2024-02-29T00:00:00.000Z'), ctime: '00:00:00.000000000', cdatetime: new Date('2024-02-29T12:34:56.789Z'), cduration: '1d2h30m', cuuid4: '550e8400-e29b-41d4-a716-446655440000', cuuid7: '018fad5d-f37a-7c94-a716-446655440000', cidentityid: '018fad5d-f37a-7c94-a716-446655440001', coptint4: Option.some(7), coptutf8: Option.none('Utf8')}]);
    });

});
