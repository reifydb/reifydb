// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
// Generated from: comparison.tap.md

import {beforeAll, describe, expect, it} from 'vitest';
import {Client, HttpClient} from "../../../src";
import {Option, Shape, BlobValue, BooleanValue, DateTimeValue, DateValue, DecimalValue, DurationValue, Float4Value, Float8Value, IdentityIdValue, Int16Value, Int1Value, Int2Value, Int4Value, Int8Value, TimeValue, Uint16Value, Uint1Value, Uint2Value, Uint4Value, Uint8Value, Utf8Value, Uuid4Value, Uuid7Value} from "@reifydb/core";

describe('Comparison (HTTP)', () => {
    let client: HttpClient;

    beforeAll(async () => {
        client = Client.connectHttp(process.env.REIFYDB_HTTP_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    });

    it('bool_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_bool CASCADE; CREATE NAMESPACE cmp_http_bool; CREATE TABLE cmp_http_bool::o { id: int4, v: Option(bool) }; INSERT cmp_http_bool::o [{ id: 1, v: true }, { id: 2, v: false }, { id: 3, v: true }, { id: 9, v: none }]; OUTPUT FROM cmp_http_bool::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.boolean())})]
        );
        const params = {p: BooleanValue.parse('false')};
        const eq = await client.query(
            "FROM cmp_http_bool::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.boolean())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(false)}]);
        const ne = await client.query(
            "FROM cmp_http_bool::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.boolean())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(true)}, {'#rownum': 3, id: 3, v: Option.some(true)}]);
        const not_eq = await client.query(
            "FROM cmp_http_bool::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.boolean())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(true)}, {'#rownum': 3, id: 3, v: Option.some(true)}]);
    });

    it('int1_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_int1 CASCADE; CREATE NAMESPACE cmp_http_int1; CREATE TABLE cmp_http_int1::o { id: int4, v: Option(int1) }; INSERT cmp_http_int1::o [{ id: 1, v: -128 }, { id: 2, v: 0 }, { id: 3, v: 127 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_int1::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int1())})]
        );
        const params = {p: Int1Value.parse('0')};
        const eq = await client.query(
            "FROM cmp_http_int1::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int1())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0)}]);
        const ne = await client.query(
            "FROM cmp_http_int1::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int1())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-128)}, {'#rownum': 3, id: 3, v: Option.some(127)}]);
        const not_eq = await client.query(
            "FROM cmp_http_int1::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int1())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-128)}, {'#rownum': 3, id: 3, v: Option.some(127)}]);
    });

    it('int2_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_int2 CASCADE; CREATE NAMESPACE cmp_http_int2; CREATE TABLE cmp_http_int2::o { id: int4, v: Option(int2) }; INSERT cmp_http_int2::o [{ id: 1, v: -32768 }, { id: 2, v: 0 }, { id: 3, v: 32767 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_int2::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int2())})]
        );
        const params = {p: Int2Value.parse('0')};
        const eq = await client.query(
            "FROM cmp_http_int2::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int2())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0)}]);
        const ne = await client.query(
            "FROM cmp_http_int2::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int2())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-32768)}, {'#rownum': 3, id: 3, v: Option.some(32767)}]);
        const not_eq = await client.query(
            "FROM cmp_http_int2::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int2())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-32768)}, {'#rownum': 3, id: 3, v: Option.some(32767)}]);
    });

    it('int4_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_int4 CASCADE; CREATE NAMESPACE cmp_http_int4; CREATE TABLE cmp_http_int4::o { id: int4, v: Option(int4) }; INSERT cmp_http_int4::o [{ id: 1, v: -2147483648 }, { id: 2, v: 0 }, { id: 3, v: 2147483647 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_int4::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())})]
        );
        const params = {p: Int4Value.parse('0')};
        const eq = await client.query(
            "FROM cmp_http_int4::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0)}]);
        const ne = await client.query(
            "FROM cmp_http_int4::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-2147483648)}, {'#rownum': 3, id: 3, v: Option.some(2147483647)}]);
        const not_eq = await client.query(
            "FROM cmp_http_int4::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int4())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-2147483648)}, {'#rownum': 3, id: 3, v: Option.some(2147483647)}]);
    });

    it('int8_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_int8 CASCADE; CREATE NAMESPACE cmp_http_int8; CREATE TABLE cmp_http_int8::o { id: int4, v: Option(int8) }; INSERT cmp_http_int8::o [{ id: 1, v: -9223372036854775808 }, { id: 2, v: 0 }, { id: 3, v: 9223372036854775807 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_int8::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int8())})]
        );
        const params = {p: Int8Value.parse('0')};
        const eq = await client.query(
            "FROM cmp_http_int8::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int8())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0n)}]);
        const ne = await client.query(
            "FROM cmp_http_int8::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int8())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-9223372036854775808n)}, {'#rownum': 3, id: 3, v: Option.some(9223372036854775807n)}]);
        const not_eq = await client.query(
            "FROM cmp_http_int8::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int8())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-9223372036854775808n)}, {'#rownum': 3, id: 3, v: Option.some(9223372036854775807n)}]);
    });

    it('int16_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_int16 CASCADE; CREATE NAMESPACE cmp_http_int16; CREATE TABLE cmp_http_int16::o { id: int4, v: Option(int16) }; INSERT cmp_http_int16::o [{ id: 1, v: -170141183460469231731687303715884105728 }, { id: 2, v: 0 }, { id: 3, v: 170141183460469231731687303715884105727 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_int16::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int16())})]
        );
        const params = {p: Int16Value.parse('0')};
        const eq = await client.query(
            "FROM cmp_http_int16::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int16())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0n)}]);
        const ne = await client.query(
            "FROM cmp_http_int16::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int16())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-170141183460469231731687303715884105728n)}, {'#rownum': 3, id: 3, v: Option.some(170141183460469231731687303715884105727n)}]);
        const not_eq = await client.query(
            "FROM cmp_http_int16::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.int16())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(-170141183460469231731687303715884105728n)}, {'#rownum': 3, id: 3, v: Option.some(170141183460469231731687303715884105727n)}]);
    });

    it('uint1_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uint1 CASCADE; CREATE NAMESPACE cmp_http_uint1; CREATE TABLE cmp_http_uint1::o { id: int4, v: Option(uint1) }; INSERT cmp_http_uint1::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 255 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uint1::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint1())})]
        );
        const params = {p: Uint1Value.parse('1')};
        const eq = await client.query(
            "FROM cmp_http_uint1::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint1())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(1)}]);
        const ne = await client.query(
            "FROM cmp_http_uint1::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint1())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(255)}]);
        const not_eq = await client.query(
            "FROM cmp_http_uint1::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint1())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(255)}]);
    });

    it('uint2_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uint2 CASCADE; CREATE NAMESPACE cmp_http_uint2; CREATE TABLE cmp_http_uint2::o { id: int4, v: Option(uint2) }; INSERT cmp_http_uint2::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 65535 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uint2::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint2())})]
        );
        const params = {p: Uint2Value.parse('1')};
        const eq = await client.query(
            "FROM cmp_http_uint2::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint2())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(1)}]);
        const ne = await client.query(
            "FROM cmp_http_uint2::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint2())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(65535)}]);
        const not_eq = await client.query(
            "FROM cmp_http_uint2::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint2())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(65535)}]);
    });

    it('uint4_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uint4 CASCADE; CREATE NAMESPACE cmp_http_uint4; CREATE TABLE cmp_http_uint4::o { id: int4, v: Option(uint4) }; INSERT cmp_http_uint4::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 4294967295 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uint4::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint4())})]
        );
        const params = {p: Uint4Value.parse('1')};
        const eq = await client.query(
            "FROM cmp_http_uint4::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint4())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(1)}]);
        const ne = await client.query(
            "FROM cmp_http_uint4::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint4())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(4294967295)}]);
        const not_eq = await client.query(
            "FROM cmp_http_uint4::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint4())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0)}, {'#rownum': 3, id: 3, v: Option.some(4294967295)}]);
    });

    it('uint8_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uint8 CASCADE; CREATE NAMESPACE cmp_http_uint8; CREATE TABLE cmp_http_uint8::o { id: int4, v: Option(uint8) }; INSERT cmp_http_uint8::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 18446744073709551615 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uint8::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint8())})]
        );
        const params = {p: Uint8Value.parse('1')};
        const eq = await client.query(
            "FROM cmp_http_uint8::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint8())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(1n)}]);
        const ne = await client.query(
            "FROM cmp_http_uint8::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint8())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0n)}, {'#rownum': 3, id: 3, v: Option.some(18446744073709551615n)}]);
        const not_eq = await client.query(
            "FROM cmp_http_uint8::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint8())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0n)}, {'#rownum': 3, id: 3, v: Option.some(18446744073709551615n)}]);
    });

    it('uint16_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uint16 CASCADE; CREATE NAMESPACE cmp_http_uint16; CREATE TABLE cmp_http_uint16::o { id: int4, v: Option(uint16) }; INSERT cmp_http_uint16::o [{ id: 1, v: 0 }, { id: 2, v: 1 }, { id: 3, v: 340282366920938463463374607431768211455 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uint16::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint16())})]
        );
        const params = {p: Uint16Value.parse('1')};
        const eq = await client.query(
            "FROM cmp_http_uint16::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint16())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(1n)}]);
        const ne = await client.query(
            "FROM cmp_http_uint16::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint16())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0n)}, {'#rownum': 3, id: 3, v: Option.some(340282366920938463463374607431768211455n)}]);
        const not_eq = await client.query(
            "FROM cmp_http_uint16::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uint16())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0n)}, {'#rownum': 3, id: 3, v: Option.some(340282366920938463463374607431768211455n)}]);
    });

    it('float4_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_float4 CASCADE; CREATE NAMESPACE cmp_http_float4; CREATE TABLE cmp_http_float4::o { id: int4, v: Option(float4) }; INSERT cmp_http_float4::o [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 1.5 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_float4::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float4())})]
        );
        const params = {p: Float4Value.parse('-0.0')};
        const eq = await client.query(
            "FROM cmp_http_float4::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float4())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0)}]);
        const ne = await client.query(
            "FROM cmp_http_float4::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float4())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0.5)}, {'#rownum': 3, id: 3, v: Option.some(1.5)}]);
        const not_eq = await client.query(
            "FROM cmp_http_float4::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float4())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0.5)}, {'#rownum': 3, id: 3, v: Option.some(1.5)}]);
    });

    it('float8_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_float8 CASCADE; CREATE NAMESPACE cmp_http_float8; CREATE TABLE cmp_http_float8::o { id: int4, v: Option(float8) }; INSERT cmp_http_float8::o [{ id: 1, v: 0.5 }, { id: 2, v: -0.0 }, { id: 3, v: 0.000000000001 }, { id: 9, v: none }]; OUTPUT FROM cmp_http_float8::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float8())})]
        );
        const params = {p: Float8Value.parse('-0.0')};
        const eq = await client.query(
            "FROM cmp_http_float8::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float8())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(0)}]);
        const ne = await client.query(
            "FROM cmp_http_float8::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float8())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0.5)}, {'#rownum': 3, id: 3, v: Option.some(1e-12)}]);
        const not_eq = await client.query(
            "FROM cmp_http_float8::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.float8())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(0.5)}, {'#rownum': 3, id: 3, v: Option.some(1e-12)}]);
    });

    it('decimal_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_decimal CASCADE; CREATE NAMESPACE cmp_http_decimal; CREATE TABLE cmp_http_decimal::o { id: int4, v: Option(decimal) }; INSERT cmp_http_decimal::o [{ id: 1, v: '1.5' }, { id: 2, v: '-0.25' }, { id: 3, v: '12345678901234567890.123456789' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_decimal::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        const params = {p: DecimalValue.parse('-0.25')};
        const eq = await client.query(
            "FROM cmp_http_decimal::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('-0.2500000000')}]);
        const ne = await client.query(
            "FROM cmp_http_decimal::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('1.5000000000')}, {'#rownum': 3, id: 3, v: Option.some('12345678901234567890.1234567890')}]);
        const not_eq = await client.query(
            "FROM cmp_http_decimal::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('1.5000000000')}, {'#rownum': 3, id: 3, v: Option.some('12345678901234567890.1234567890')}]);
    });

    it('decimal_scaled_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_decimal_scaled CASCADE; CREATE NAMESPACE cmp_http_decimal_scaled; CREATE TABLE cmp_http_decimal_scaled::o { id: int4, v: Option(decimal(30, 3)) }; INSERT cmp_http_decimal_scaled::o [{ id: 1, v: '1.5' }, { id: 2, v: '-0.25' }, { id: 3, v: '12345678901234567890.123' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_decimal_scaled::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        const params = {p: DecimalValue.parse('-0.25')};
        const eq = await client.query(
            "FROM cmp_http_decimal_scaled::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('-0.250')}]);
        const ne = await client.query(
            "FROM cmp_http_decimal_scaled::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('1.500')}, {'#rownum': 3, id: 3, v: Option.some('12345678901234567890.123')}]);
        const not_eq = await client.query(
            "FROM cmp_http_decimal_scaled::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.decimal())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('1.500')}, {'#rownum': 3, id: 3, v: Option.some('12345678901234567890.123')}]);
    });

    it('utf8_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_utf8 CASCADE; CREATE NAMESPACE cmp_http_utf8; CREATE TABLE cmp_http_utf8::o { id: int4, v: Option(utf8) }; INSERT cmp_http_utf8::o [{ id: 1, v: '' }, { id: 2, v: 'héllo wörld' }, { id: 3, v: '日本語' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_utf8::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())})]
        );
        const params = {p: Utf8Value.parse('héllo wörld')};
        const eq = await client.query(
            "FROM cmp_http_utf8::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('héllo wörld')}]);
        const ne = await client.query(
            "FROM cmp_http_utf8::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('')}, {'#rownum': 3, id: 3, v: Option.some('日本語')}]);
        const not_eq = await client.query(
            "FROM cmp_http_utf8::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.utf8())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('')}, {'#rownum': 3, id: 3, v: Option.some('日本語')}]);
    });

    it('blob_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_blob CASCADE; CREATE NAMESPACE cmp_http_blob; CREATE TABLE cmp_http_blob::o { id: int4, v: Option(blob) }; INSERT cmp_http_blob::o [{ id: 1, v: blob::hex('') }, { id: 2, v: blob::hex('00ff10') }, { id: 3, v: blob::hex('ff') }, { id: 9, v: none }]; OUTPUT FROM cmp_http_blob::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.blob())})]
        );
        const params = {p: BlobValue.parse('0x00ff10')};
        const eq = await client.query(
            "FROM cmp_http_blob::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.blob())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(new Uint8Array([0x00, 0xff, 0x10]))}]);
        const ne = await client.query(
            "FROM cmp_http_blob::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.blob())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Uint8Array([]))}, {'#rownum': 3, id: 3, v: Option.some(new Uint8Array([0xff]))}]);
        const not_eq = await client.query(
            "FROM cmp_http_blob::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.blob())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Uint8Array([]))}, {'#rownum': 3, id: 3, v: Option.some(new Uint8Array([0xff]))}]);
    });

    it('date_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_date CASCADE; CREATE NAMESPACE cmp_http_date; CREATE TABLE cmp_http_date::o { id: int4, v: Option(date) }; INSERT cmp_http_date::o [{ id: 1, v: '1970-01-01' }, { id: 2, v: '2024-02-29' }, { id: 3, v: '2100-12-31' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_date::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.date())})]
        );
        const params = {p: DateValue.parse('2024-02-29')};
        const eq = await client.query(
            "FROM cmp_http_date::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.date())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(new Date('2024-02-29T00:00:00.000Z'))}]);
        const ne = await client.query(
            "FROM cmp_http_date::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.date())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Date('1970-01-01T00:00:00.000Z'))}, {'#rownum': 3, id: 3, v: Option.some(new Date('2100-12-31T00:00:00.000Z'))}]);
        const not_eq = await client.query(
            "FROM cmp_http_date::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.date())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Date('1970-01-01T00:00:00.000Z'))}, {'#rownum': 3, id: 3, v: Option.some(new Date('2100-12-31T00:00:00.000Z'))}]);
    });

    it('time_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_time CASCADE; CREATE NAMESPACE cmp_http_time; CREATE TABLE cmp_http_time::o { id: int4, v: Option(time) }; INSERT cmp_http_time::o [{ id: 1, v: '00:00:00' }, { id: 2, v: '12:34:56.789' }, { id: 3, v: '23:59:59.999999999' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_time::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.time())})]
        );
        const params = {p: TimeValue.parse('12:34:56.789')};
        const eq = await client.query(
            "FROM cmp_http_time::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.time())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('12:34:56.789000000')}]);
        const ne = await client.query(
            "FROM cmp_http_time::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.time())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('00:00:00.000000000')}, {'#rownum': 3, id: 3, v: Option.some('23:59:59.999999999')}]);
        const not_eq = await client.query(
            "FROM cmp_http_time::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.time())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('00:00:00.000000000')}, {'#rownum': 3, id: 3, v: Option.some('23:59:59.999999999')}]);
    });

    it('datetime_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_datetime CASCADE; CREATE NAMESPACE cmp_http_datetime; CREATE TABLE cmp_http_datetime::o { id: int4, v: Option(datetime) }; INSERT cmp_http_datetime::o [{ id: 1, v: '1970-01-01T00:00:00Z' }, { id: 2, v: '2024-02-29T12:34:56.789Z' }, { id: 3, v: '2100-12-31T23:59:59Z' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_datetime::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.datetime())})]
        );
        const params = {p: DateTimeValue.parse('2024-02-29T12:34:56.789Z')};
        const eq = await client.query(
            "FROM cmp_http_datetime::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.datetime())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some(new Date('2024-02-29T12:34:56.789Z'))}]);
        const ne = await client.query(
            "FROM cmp_http_datetime::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.datetime())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Date('1970-01-01T00:00:00.000Z'))}, {'#rownum': 3, id: 3, v: Option.some(new Date('2100-12-31T23:59:59.000Z'))}]);
        const not_eq = await client.query(
            "FROM cmp_http_datetime::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.datetime())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some(new Date('1970-01-01T00:00:00.000Z'))}, {'#rownum': 3, id: 3, v: Option.some(new Date('2100-12-31T23:59:59.000Z'))}]);
    });

    it('duration_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_duration CASCADE; CREATE NAMESPACE cmp_http_duration; CREATE TABLE cmp_http_duration::o { id: int4, v: Option(duration) }; INSERT cmp_http_duration::o [{ id: 1, v: '0s' }, { id: 2, v: '1d2h30m' }, { id: 3, v: '30m' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_duration::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.duration())})]
        );
        const params = {p: DurationValue.parse('1d2h30m')};
        const eq = await client.query(
            "FROM cmp_http_duration::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.duration())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('1d2h30m')}]);
        const ne = await client.query(
            "FROM cmp_http_duration::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.duration())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('0s')}, {'#rownum': 3, id: 3, v: Option.some('30m')}]);
        const not_eq = await client.query(
            "FROM cmp_http_duration::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.duration())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('0s')}, {'#rownum': 3, id: 3, v: Option.some('30m')}]);
    });

    it('uuid4_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uuid4 CASCADE; CREATE NAMESPACE cmp_http_uuid4; CREATE TABLE cmp_http_uuid4::o { id: int4, v: Option(uuid4) }; INSERT cmp_http_uuid4::o [{ id: 1, v: '550e8400-e29b-41d4-a716-446655440000' }, { id: 2, v: '550e8400-e29b-41d4-a716-446655440001' }, { id: 3, v: '550e8400-e29b-41d4-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uuid4::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid4())})]
        );
        const params = {p: Uuid4Value.parse('550e8400-e29b-41d4-a716-446655440001')};
        const eq = await client.query(
            "FROM cmp_http_uuid4::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid4())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('550e8400-e29b-41d4-a716-446655440001')}]);
        const ne = await client.query(
            "FROM cmp_http_uuid4::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid4())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('550e8400-e29b-41d4-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('550e8400-e29b-41d4-a716-446655440002')}]);
        const not_eq = await client.query(
            "FROM cmp_http_uuid4::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid4())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('550e8400-e29b-41d4-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('550e8400-e29b-41d4-a716-446655440002')}]);
    });

    it('uuid7_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_uuid7 CASCADE; CREATE NAMESPACE cmp_http_uuid7; CREATE TABLE cmp_http_uuid7::o { id: int4, v: Option(uuid7) }; INSERT cmp_http_uuid7::o [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }, { id: 2, v: '018fad5d-f37a-7c94-a716-446655440001' }, { id: 3, v: '018fad5d-f37a-7c94-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_uuid7::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid7())})]
        );
        const params = {p: Uuid7Value.parse('018fad5d-f37a-7c94-a716-446655440001')};
        const eq = await client.query(
            "FROM cmp_http_uuid7::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid7())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('018fad5d-f37a-7c94-a716-446655440001')}]);
        const ne = await client.query(
            "FROM cmp_http_uuid7::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid7())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('018fad5d-f37a-7c94-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('018fad5d-f37a-7c94-a716-446655440002')}]);
        const not_eq = await client.query(
            "FROM cmp_http_uuid7::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.uuid7())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('018fad5d-f37a-7c94-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('018fad5d-f37a-7c94-a716-446655440002')}]);
    });

    it('identity_id_param', async () => {
        await client.admin(
            "DROP NAMESPACE IF EXISTS cmp_http_identity_id CASCADE; CREATE NAMESPACE cmp_http_identity_id; CREATE TABLE cmp_http_identity_id::o { id: int4, v: Option(identity_id) }; INSERT cmp_http_identity_id::o [{ id: 1, v: '018fad5d-f37a-7c94-a716-446655440000' }, { id: 2, v: '018fad5d-f37a-7c94-a716-446655440001' }, { id: 3, v: '018fad5d-f37a-7c94-a716-446655440002' }, { id: 9, v: none }]; OUTPUT FROM cmp_http_identity_id::o SORT { id: asc }",
            null,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.identityid())})]
        );
        const params = {p: IdentityIdValue.parse('018fad5d-f37a-7c94-a716-446655440001')};
        const eq = await client.query(
            "FROM cmp_http_identity_id::o FILTER { v == $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.identityid())})]
        );
        expect(eq).toHaveLength(1);
        expect(eq[0]).toEqual([{'#rownum': 2, id: 2, v: Option.some('018fad5d-f37a-7c94-a716-446655440001')}]);
        const ne = await client.query(
            "FROM cmp_http_identity_id::o FILTER { v != $p } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.identityid())})]
        );
        expect(ne).toHaveLength(1);
        expect(ne[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('018fad5d-f37a-7c94-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('018fad5d-f37a-7c94-a716-446655440002')}]);
        const not_eq = await client.query(
            "FROM cmp_http_identity_id::o FILTER { not (v == $p) } SORT { id: asc }",
            params,
            [Shape.object({id: Shape.int4(), v: Shape.option(Shape.identityid())})]
        );
        expect(not_eq).toHaveLength(1);
        expect(not_eq[0]).toEqual([{'#rownum': 1, id: 1, v: Option.some('018fad5d-f37a-7c94-a716-446655440000')}, {'#rownum': 3, id: 3, v: Option.some('018fad5d-f37a-7c94-a716-446655440002')}]);
    });
});
