// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {beforeAll, describe, expect, it} from "vitest";
import {Client, JsonWsClient} from "../../../src";
import {Shape, Utf8Value, Int4Value, InferShape} from "@reifydb/core";

// The JSON transports decode from text payloads rather than the binary frame, so the
// shape has to be honoured on a separate code path from the http/ws suites.
const versionShape = Shape.object({
    name: Shape.string(),
    type: Shape.string(),
    version: Shape.string(),
    description: Shape.string()
});

type VersionRow = InferShape<typeof versionShape>;

const ns = `shape_conv_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

describe('Shape Type Conversion [json-ws]', () => {
    let client: JsonWsClient;

    beforeAll(async () => {
        client = await Client.connectJsonWs(process.env.REIFYDB_WS_URL, {timeoutMs: 10000, token: process.env.REIFYDB_TOKEN});
    });

    describe('Primitive Shape Conversion', () => {
        it('should convert Value objects to primitives when using primitive shape', async () => {
            const result = await client.query("FROM system::versions TAKE 1", null, [versionShape]);

            expect(result).toHaveLength(1);
            const rows = result[0];
            expect(rows.length).toBeGreaterThan(0);

            const row = rows[0] as VersionRow;
            expect(typeof row.name).toBe('string');
            expect(typeof row.type).toBe('string');
            expect(typeof row.version).toBe('string');
            expect(typeof row.description).toBe('string');
            expect(row.name).not.toBeInstanceOf(Utf8Value);
            expect(row.name.valueOf()).toBe(row.name);
        }, 10000);

        it('should handle mixed primitive types correctly', async () => {
            const shape = Shape.object({
                str_val: Shape.string(),
                int_val: Shape.int4(),
                bool_val: Shape.boolean(),
                float_val: Shape.float8()
            });

            const result = await client.admin(
                "MAP { str_val: 'test', int_val: 42, bool_val: true, float_val: cast(3.14, float8) }",
                null,
                [shape]
            );

            const row = result[0][0];
            expect(typeof row.strVal).toBe('string');
            expect(row.strVal).toBe('test');
            expect(typeof row.intVal).toBe('number');
            expect(row.intVal).toBe(42);
            expect(typeof row.boolVal).toBe('boolean');
            expect(row.boolVal).toBe(true);
            expect(typeof row.floatVal).toBe('number');
            expect(row.floatVal).toBeCloseTo(3.14);
        }, 10000);

        it('should handle bigint types correctly', async () => {
            const shape = Shape.object({big_val: Shape.int8(), another_val: Shape.int8()});

            const result = await client.admin(
                "MAP { big_val: 9223372036854775807, another_val: 1 }",
                null,
                [shape]
            );

            const row = result[0][0];
            expect(typeof row.bigVal).toBe('bigint');
            expect(row.bigVal).toBe(BigInt("9223372036854775807"));
            expect(typeof row.anotherVal).toBe('bigint');
            expect(row.anotherVal).toBe(BigInt(1));
        }, 10000);
    });

    describe('Value Shape Preservation', () => {
        it('should keep Value objects when using value shape', async () => {
            const valueShape = Shape.object({name: Shape.utf8Value(), count: Shape.int4Value()});

            const result = await client.admin("MAP { name: 'test', count: 42 }", null, [valueShape]);

            const row = result[0][0];
            expect(row.name).toBeInstanceOf(Utf8Value);
            // 42 fits in Int1 range, so the server sends it as Int1
            // The server sends 42 as Int1, the narrowest type it fits. The value shape names the
            // type the caller wants to hold, and Int4 holds every Int1, so the shape is honoured.
            expect(row.count).toBeInstanceOf(Int4Value);
            expect(row.count.type).toBe('Int4');
            expect(row.name.valueOf()).toBe('test');
            expect(row.count.valueOf()).toBe(42);
        }, 10000);
    });

    describe('Option Shape Conversion', () => {
        const optionShape = Shape.object({id: Shape.int4(), value: Shape.option(Shape.int4())});

        beforeAll(async () => {
            await client.admin(`create namespace ${ns}`, null, []).catch(() => undefined);
            await client.admin(`create table ${ns}::opt { id: int4, value: Option(int4) }`, null, []);
            await client.command(`insert ${ns}::opt [{ id: 1, value: 42 }, { id: 2, value: none }]`, null, []);
        }, 20000);

        it('a present optional value decodes as Some carrying the value', async () => {
            const result = await client.query(`from ${ns}::opt filter id == 1`, null, [optionShape]);

            const row = result[0][0];
            expect(row.value.isSome()).toBe(true);
            expect(row.value.unwrap()).toBe(42);
        }, 10000);

        it('an absent optional value decodes as None rather than a zero', async () => {
            const result = await client.query(`from ${ns}::opt filter id == 2`, null, [optionShape]);

            const row = result[0][0];
            expect(row.value.isNone()).toBe(true);
            expect(() => row.value.unwrap()).toThrow();
        }, 10000);

        it('a shape that disagrees with the column type is rejected rather than decoded wrong', async () => {
            const wrong = Shape.object({id: Shape.string()});

            await expect(client.query(`from ${ns}::opt`, null, [wrong]))
                .rejects.toMatchObject({name: 'ShapeMismatch'});
        }, 10000);
    });

    describe('Without Shape', () => {
        it('should return Value objects when no shape is provided', async () => {
            const result = await client.query("FROM system::versions TAKE 1", null, []);

            const row: any = result[0][0];
            expect(row.name).toBeInstanceOf(Utf8Value);
            expect(row.version).toBeInstanceOf(Utf8Value);
        }, 10000);
    });
});
