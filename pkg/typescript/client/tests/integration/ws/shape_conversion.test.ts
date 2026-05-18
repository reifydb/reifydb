// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {Client, WsClient} from "../../../src";
import {wait_for_database} from "../setup";
import {Shape, Utf8Value, Int4Value, Int1Value, InferShape} from "@reifydb/core";

// Define the shape once
const versionShape = Shape.object({
    name: Shape.string(),
    type: Shape.string(),
    version: Shape.string(),
    description: Shape.string()
});

// Infer the TypeScript type from the shape
type VersionRow = InferShape<typeof versionShape>;

describe.each([
    {format: "frames"},
    {format: "rbcf"},
] as const)('Shape Type Conversion [$format]', ({format}) => {
    let ws_client: WsClient;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    beforeEach(async () => {
        try {
            ws_client = await Client.connect_ws(process.env.REIFYDB_WS_URL, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN,
                format,
            });
        } catch (error) {
            console.error('❌ WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);

    afterEach(async () => {
        if (ws_client) {
            try {
                ws_client.disconnect();
            } catch (error) {
                console.error('⚠️ Error during disconnect:', error);
            }
            ws_client = null;
        }
    });

    describe('Primitive Shape Conversion', () => {
        it('should convert Value objects to primitives when using primitive shape', async () => {
            const result = await ws_client.query(
                "FROM system::versions TAKE 1",
                null,
                [versionShape]
            );

            expect(result).toBeDefined();
            expect(result).toHaveLength(1);
            const frames = result[0];
            expect(frames).toBeDefined();
            expect(frames!.length).toBeGreaterThan(0);
            
            const row = frames![0] as VersionRow;  // Type assertion shows the expected type
                
                // Check that values are primitives, not Value objects
                expect(typeof row.name).toBe('string');
                expect(typeof row.type).toBe('string');
                expect(typeof row.version).toBe('string');
                expect(typeof row.description).toBe('string');
                
                // Ensure they are NOT Value objects
                expect(row.name).not.toBeInstanceOf(Utf8Value);
                expect(row.type).not.toBeInstanceOf(Utf8Value);
                expect(row.version).not.toBeInstanceOf(Utf8Value);
                expect(row.description).not.toBeInstanceOf(Utf8Value);
                
                // Verify they don't have valueOf method (primitive strings don't)
                expect(typeof row.name.valueOf).toBe('function'); // strings have valueOf
                // But calling valueOf should return the same primitive
                expect(row.name.valueOf()).toBe(row.name);
        }, 5000);

        it('should handle mixed primitive types correctly', async () => {
            const shape = Shape.object({
                str_val: Shape.string(),
                int_val: Shape.int4(),
                bool_val: Shape.boolean(),
                float_val: Shape.float8()
            });

            const result = await ws_client.admin(
                "MAP { str_val: 'test', int_val: 42, bool_val: true, float_val: 3.14 }",
                null,
                [shape]
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            
            // Verify primitive types
            expect(typeof row.str_val).toBe('string');
            expect(row.str_val).toBe('test');
            
            expect(typeof row.int_val).toBe('number');
            expect(row.int_val).toBe(42);
            
            expect(typeof row.bool_val).toBe('boolean');
            expect(row.bool_val).toBe(true);
            
            expect(typeof row.float_val).toBe('number');
            expect(row.float_val).toBeCloseTo(3.14);
        }, 5000);

        it('should handle bigint types correctly', async () => {
            const shape = Shape.object({
                big_val: Shape.int8(),
                another_val: Shape.int8()
            });

            const result = await ws_client.admin(
                "MAP { big_val: 9223372036854775807, another_val: 1 }",
                null,
                [shape]
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];

            // Verify bigint types
            expect(typeof row.big_val).toBe('bigint');
            expect(row.big_val).toBe(BigInt("9223372036854775807"));

            expect(typeof row.another_val).toBe('bigint');
            expect(row.another_val).toBe(BigInt(1));
        }, 5000);
    });

    describe('Value Shape Preservation', () => {
        it('should keep Value objects when using value shape', async () => {
            const valueShape = Shape.object({
                name: Shape.utf8Value(),
                count: Shape.int4Value()
            });

            const result = await ws_client.admin(
                "MAP { name: 'test', count: 42 }",
                null,
                [valueShape]
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            
            // Check that values are Value objects
            expect(row.name).toBeInstanceOf(Utf8Value);
            // The number 42 fits in Int1 range, so it's encoded as Int1Value
            expect(row.count).toBeInstanceOf(Int1Value); // Check it's an Int1Value object
            
            // Verify they have valueOf methods
            expect(typeof row.name.valueOf).toBe('function');
            expect(typeof row.count.valueOf).toBe('function');
            
            // Check the actual values
            expect(row.name.valueOf()).toBe('test');
            expect(row.count.valueOf()).toBe(42);
        }, 5000);
    });

    describe('Without Shape (backward compatibility)', () => {
        it('should return Value objects when no shape is provided', async () => {
            const result = await ws_client.query(
                "FROM system::versions TAKE 1",
                null,
                [] // No shape provided
            );

            expect(result).toBeDefined();
            expect(result).toHaveLength(1);
            // @ts-ignore
            const frames = result[0];
            expect(frames).toBeDefined();
            // @ts-ignore
            expect(frames!.length).toBeGreaterThan(0);
            
            // @ts-ignore
            const row = frames![0];
            
            // Without shape, should get Value objects
            expect(row.name).toBeInstanceOf(Utf8Value);
            expect(row.type).toBeInstanceOf(Utf8Value);
            expect(row.version).toBeInstanceOf(Utf8Value);
            expect(row.description).toBeInstanceOf(Utf8Value);
        }, 5000);
    });
});