/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient, Schema, InferPrimitiveSchemaResult, BidirectionalSchema} from "../../../src";
import {LEGACY_SCHEMA, PRIMITIVE_RESULT_SCHEMA} from "../test-helpers";
import {ObjectSchemaNode, OptionalSchemaNode} from "@reifydb/core";

describe('Primitive Result Types', () => {
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

    describe('command with primitive result types', () => {
        it('should return primitive boolean result', async () => {
            const frames = await wsClient.command(
                'MAP $value as result',
                {value: true},
                [Schema.object({result: Schema.boolean()})]
            );


            console.log('Raw frames:', JSON.stringify(frames, null, 2));
            console.log('Result type:', typeof frames[0][0].result);
            console.log('Result value:', frames[0][0].result);
            console.log('Result constructor:', frames[0][0].result?.constructor?.name);

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('boolean');
            expect(frames[0][0].result).toBe(true);
        }, 1000);

        it('should return primitive number result for small integer', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $num as result',
                {num: 42},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive number
            expect(typeof frames[0][0].result.value).toBe('number');
            expect(frames[0][0].result.value).toBe(42);
        }, 1000);

        it('should return primitive number result for medium integer', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $num as result',
                {num: 30000},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive number
            expect(typeof frames[0][0].result.value).toBe('number');
            expect(frames[0][0].result.value).toBe(30000);
        }, 1000);

        it('should return primitive number result for large integer', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $num as result',
                {num: 2000000},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive number
            expect(typeof frames[0][0].result.value).toBe('number');
            expect(frames[0][0].result.value).toBe(2000000);
        }, 1000);

        it('should return primitive bigint result for very large integer', async () => {
            const frames = await wsClient.command<[{ result: bigint }]>(
                'MAP $num as result',
                {num: 3000000000},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive bigint
            expect(typeof frames[0][0].result.value).toBe('bigint');
            expect(frames[0][0].result.value).toBe(BigInt(3000000000));
        }, 1000);

        it('should return primitive number result for float', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $num as result',
                {num: 3.14159},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive number
            expect(typeof frames[0][0].result.value).toBe('number');
            expect(frames[0][0].result.value).toBeCloseTo(3.14159);
        }, 1000);

        it('should return primitive string result', async () => {
            const frames = await wsClient.command<[{ result: string }]>(
                'MAP $text as result',
                {text: "hello primitive result"},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive string
            expect(typeof frames[0][0].result.value).toBe('string');
            expect(frames[0][0].result.value).toBe("hello primitive result");
        }, 1000);

        it('should return primitive bigint result for explicit bigint', async () => {
            const frames = await wsClient.command<[{ result: bigint }]>(
                'MAP $bignum as result',
                {bignum: BigInt("9223372036854775807")},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive bigint
            expect(typeof frames[0][0].result.value).toBe('bigint');
            expect(frames[0][0].result.value).toBe(BigInt("9223372036854775807"));
        }, 1000);

        it('should return primitive Date result', async () => {
            const testDate = new Date('2024-12-25T12:00:00Z');
            const frames = await wsClient.command<[{ result: Date }]>(
                'MAP $timestamp as result',
                {timestamp: testDate},
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // Value object returned, but TypeScript expects primitive Date
            expect(frames[0][0].result.value instanceof Date).toBe(true);
            expect(frames[0][0].result.value).toEqual(testDate);
        }, 1000);

        it('should return primitive undefined result for null/undefined', async () => {
            const frames1 = await wsClient.command<[{ result: undefined }]>(
                'MAP $value as result',
                {value: null},
                LEGACY_SCHEMA
            );

            const frames2 = await wsClient.command<[{ result: undefined }]>(
                'MAP $value as result',
                {value: undefined},
                LEGACY_SCHEMA
            );

            // Value object returned, but TypeScript expects primitive undefined
            expect(frames1[0][0].result.value).toBe(undefined);
            expect(frames2[0][0].result.value).toBe(undefined);
        }, 1000);

        it('should return mixed primitive result types', async () => {
            const frames = await wsClient.command<[{
                bool_val: boolean,
                int_val: number,
                str_val: string,
                float_val: number
            }]>(
                'MAP { $bool as bool_val, $int as int_val, $str as str_val, $float as float_val }',
                {
                    bool: false,
                    int: 999,
                    str: "mixed primitive results",
                    float: 2.718
                },
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            const result = frames[0][0];
            // Value objects returned, but TypeScript expects primitive types
            expect(typeof result.bool_val.value).toBe('boolean');
            expect(result.bool_val.value).toBe(false);
            expect(typeof result.int_val.value).toBe('number');
            expect(result.int_val.value).toBe(999);
            expect(typeof result.str_val.value).toBe('string');
            expect(result.str_val.value).toBe("mixed primitive results");
            expect(typeof result.float_val.value).toBe('number');
            expect(result.float_val.value).toBeCloseTo(2.718);
        }, 1000);

        it('should return primitive array result types', async () => {
            const frames = await wsClient.command<[{
                bool_result: boolean,
                int_result: number,
                str_result: string
            }]>(
                'MAP { $1 as bool_result, $2 as int_result, $3 as str_result }',
                [true, 123, "array result test"],
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            const result = frames[0][0];
            // Value objects returned, but TypeScript expects primitive types
            expect(typeof result.bool_result.value).toBe('boolean');
            expect(result.bool_result.value).toBe(true);
            expect(typeof result.int_result.value).toBe('number');
            expect(result.int_result.value).toBe(123);
            expect(typeof result.str_result.value).toBe('string');
            expect(result.str_result.value).toBe("array result test");
        }, 1000);
    });

    describe('query with primitive result types', () => {
        it('should return primitive boolean result', async () => {
            const frames = await wsClient.query(
                'MAP $value as result',
                {value: false},
                Schema.withPrimitiveResult({result: 'boolean'})
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('boolean');
            expect(frames[0][0].result).toBe(false);
        }, 1000);

        it('should return primitive number result for integer', async () => {
            const frames = await wsClient.query(
                'MAP $num as result',
                {num: 50000},
                Schema.withPrimitiveResult({result: 'number'})
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('number');
            expect(frames[0][0].result).toBe(50000);
        }, 1000);

        it('should return primitive number result for float', async () => {
            const frames = await wsClient.query(
                'MAP $pi as result',
                {pi: Math.PI},
                Schema.withPrimitiveResult({result: 'number'})
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('number');
            expect(frames[0][0].result).toBeCloseTo(Math.PI);
        }, 1000);

        it('should return primitive string result', async () => {
            const frames = await wsClient.query(
                'MAP $text as result',
                {text: "query primitive result test"},
                Schema.withPrimitiveResult({result: 'string'})
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(typeof frames[0][0].result).toBe('string');
            expect(frames[0][0].result).toBe("query primitive result test");
        }, 1000);

        it('should return primitive Date result', async () => {
            const testDate = new Date('2024-06-15T08:30:00Z');
            const frames = await wsClient.query(
                'MAP $date as result',
                {date: testDate},
                Schema.withPrimitiveResult({result: 'Date'})
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Date).toBe(true);
            expect(frames[0][0].result).toEqual(testDate);
        }, 1000);

        it('should return mixed primitive result types', async () => {
            const frames = await wsClient.query<[{
                mixed_bool: boolean,
                mixed_int: number,
                mixed_str: string
            }]>(
                'MAP { $bool_param as mixed_bool, $int_param as mixed_int, $str_param as mixed_str }',
                {
                    bool_param: true,
                    int_param: 777,
                    str_param: "query mixed primitive results"
                },
                {params: LEGACY_SCHEMA.params, result: Schema.auto()}
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            const result = frames[0][0];
            expect(typeof result.mixed_bool).toBe('boolean');
            expect(result.mixed_bool).toBe(true);
            expect(typeof result.mixed_int).toBe('number');
            expect(result.mixed_int).toBe(777);
            expect(typeof result.mixed_str).toBe('string');
            expect(result.mixed_str).toBe("query mixed primitive results");
        }, 1000);

        it('should return primitive array result types', async () => {
            const frames = await wsClient.query<[{
                first: boolean,
                second: number,
                third: string
            }]>(
                'MAP { $1 as first, $2 as second, $3 as third }',
                [false, 42.5, "query array results"],
                {params: LEGACY_SCHEMA.params, result: Schema.auto()}
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            const result = frames[0][0];
            expect(typeof result.first).toBe('boolean');
            expect(result.first).toBe(false);
            expect(typeof result.second).toBe('number');
            expect(result.second).toBe(42.5);
            expect(typeof result.third).toBe('string');
            expect(result.third).toBe("query array results");
        }, 1000);

        it('should handle complex primitive result scenarios', async () => {
            const frames = await wsClient.query<[{
                small_int: number,
                large_int: bigint,
                precise_float: number,
                text_field: string,
                is_active: boolean
            }]>(
                'MAP { $small as small_int, $large as large_int, $precise as precise_float, $text as text_field, $active as is_active }',
                {
                    small: 100,
                    large: 5000000000,
                    precise: 123.456789,
                    text: "complex primitive result scenario",
                    active: true
                },
                {params: LEGACY_SCHEMA.params, result: Schema.auto()}
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);

            const result = frames[0][0];
            expect(typeof result.small_int).toBe('number');
            expect(result.small_int).toBe(100);
            expect(typeof result.large_int).toBe('bigint');
            expect(result.large_int).toBe(BigInt(5000000000));
            expect(typeof result.precise_float).toBe('number');
            expect(result.precise_float).toBeCloseTo(123.456789);
            expect(typeof result.text_field).toBe('string');
            expect(result.text_field).toBe("complex primitive result scenario");
            expect(typeof result.is_active).toBe('boolean');
            expect(result.is_active).toBe(true);
        }, 1000);
    });
});