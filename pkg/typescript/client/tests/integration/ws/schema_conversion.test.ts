// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from "vitest";
import {Client, WsClient} from "../../../src";
import {waitForDatabase} from "../setup";
import {Schema, Utf8Value, Int4Value, Int1Value, InferSchema} from "@reifydb/core";

// Define the schema once
const versionSchema = Schema.object({
    name: Schema.string(),
    type: Schema.string(),
    version: Schema.string(),
    description: Schema.string()
});

// Infer the TypeScript type from the schema
type VersionRow = InferSchema<typeof versionSchema>;

describe('Schema Type Conversion', () => {
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

    describe('Primitive Schema Conversion', () => {
        it('should convert Value objects to primitives when using primitive schema', async () => {
            const result = await wsClient.query(
                "FROM system::versions TAKE 1",
                null,
                [versionSchema]
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
            const schema = Schema.object({
                str_val: Schema.string(),
                int_val: Schema.int4(),
                bool_val: Schema.boolean(),
                float_val: Schema.float8()
            });

            const result = await wsClient.admin(
                "MAP { str_val: 'test', int_val: 42, bool_val: true, float_val: 3.14 }",
                null,
                [schema]
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
            const schema = Schema.object({
                big_val: Schema.int8(),
                another_val: Schema.int8()
            });

            const result = await wsClient.admin(
                "MAP { big_val: 9223372036854775807, another_val: 1 }",
                null,
                [schema]
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

    describe('Value Schema Preservation', () => {
        it('should keep Value objects when using value schema', async () => {
            const valueSchema = Schema.object({
                name: Schema.utf8Value(),
                count: Schema.int4Value()
            });

            const result = await wsClient.admin(
                "MAP { name: 'test', count: 42 }",
                null,
                [valueSchema]
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

    describe('Without Schema (backward compatibility)', () => {
        it('should return Value objects when no schema is provided', async () => {
            const result = await wsClient.query(
                "FROM system::versions TAKE 1",
                null,
                [] // No schema provided
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
            
            // Without schema, should get Value objects
            expect(row.name).toBeInstanceOf(Utf8Value);
            expect(row.type).toBeInstanceOf(Utf8Value);
            expect(row.version).toBeInstanceOf(Utf8Value);
            expect(row.description).toBeInstanceOf(Utf8Value);
        }, 5000);
    });
});