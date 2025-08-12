/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {
    Client, 
    WsClient, 
    BoolValue, 
    Int1Value, 
    Int2Value, 
    Int4Value, 
    Int8Value, 
    Float8Value, 
    Utf8Value, 
    DateTimeValue, 
    UndefinedValue
} from "../../../src";


describe('Primitive Parameter Support', () => {
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

    describe('command with primitive parameters', () => {
        it('should accept primitive boolean and return BoolValue', async () => {
            const frames = await wsClient.command<[{ result: BoolValue }]>(
                'MAP $value as result',
                { value: true }, // primitive boolean
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof BoolValue).toBe(true);
            expect(frames[0][0].result.value).toBe(true);
        }, 1000);

        it('should accept primitive small integer and return Int1Value', async () => {
            const frames = await wsClient.command<[{ result: Int1Value }]>(
                'MAP $num as result',
                { num: 42 }, // primitive number in Int1 range
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int1Value).toBe(true);
            expect(frames[0][0].result.value).toBe(42);
        }, 1000);

        it('should accept primitive medium integer and return Int2Value', async () => {
            const frames = await wsClient.command<[{ result: Int2Value }]>(
                'MAP $num as result',
                { num: 30000 }, // primitive number in Int2 range
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int2Value).toBe(true);
            expect(frames[0][0].result.value).toBe(30000);
        }, 1000);

        it('should accept primitive large integer and return Int4Value', async () => {
            const frames = await wsClient.command<[{ result: Int4Value }]>(
                'MAP $num as result',
                { num: 2000000 }, // primitive number in Int4 range
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int4Value).toBe(true);
            expect(frames[0][0].result.value).toBe(2000000);
        }, 1000);

        it('should accept primitive very large integer and return Int8Value', async () => {
            const frames = await wsClient.command<[{ result: Int8Value }]>(
                'MAP $num as result',
                { num: 3000000000 }, // primitive number > Int4 range
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int8Value).toBe(true);
            expect(frames[0][0].result.value).toBe(BigInt(3000000000));
        }, 1000);

        it('should accept primitive float and return Float8Value', async () => {
            const frames = await wsClient.command<[{ result: Float8Value }]>(
                'MAP $num as result',
                { num: 3.14159 }, // primitive float
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Float8Value).toBe(true);
            expect(frames[0][0].result.value).toBeCloseTo(3.14159);
        }, 1000);

        it('should accept primitive string and return Utf8Value', async () => {
            const frames = await wsClient.command<[{ result: Utf8Value }]>(
                'MAP $text as result',
                { text: "hello primitive" }, // primitive string
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Utf8Value).toBe(true);
            expect(frames[0][0].result.value).toBe("hello primitive");
        }, 1000);

        it('should accept primitive bigint and return Int8Value', async () => {
            const frames = await wsClient.command<[{ result: Int8Value }]>(
                'MAP $bignum as result',
                { bignum: BigInt("9223372036854775807") }, // primitive bigint
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int8Value).toBe(true);
            expect(frames[0][0].result.value).toBe(BigInt("9223372036854775807"));
        }, 1000);

        it('should accept primitive Date and return DateTimeValue', async () => {
            const testDate = new Date('2024-12-25T12:00:00Z');
            const frames = await wsClient.command<[{ result: DateTimeValue }]>(
                'MAP $timestamp as result',
                { timestamp: testDate }, // primitive Date
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof DateTimeValue).toBe(true);
            expect(frames[0][0].result.value).toEqual(testDate);
        }, 1000);

        it('should accept primitive null/undefined and return UndefinedValue', async () => {
            const frames1 = await wsClient.command<[{ result: UndefinedValue }]>(
                'MAP $value as result',
                { value: null }, // primitive null
                LEGACY_SCHEMA
            );

            const frames2 = await wsClient.command<[{ result: UndefinedValue }]>(
                'MAP $value as result',
                { value: undefined }, // primitive undefined
                LEGACY_SCHEMA
            );

            expect(frames1[0][0].result instanceof UndefinedValue).toBe(true);
            expect(frames1[0][0].result.value).toBe(undefined);
            expect(frames2[0][0].result instanceof UndefinedValue).toBe(true);
            expect(frames2[0][0].result.value).toBe(undefined);
        }, 1000);

        it('should accept mixed primitive and Value object parameters', async () => {
            const frames = await wsClient.command<[{
                bool_val: BoolValue,
                int_val: Int4Value,
                str_val: Utf8Value,
                float_val: Float8Value
            }]>(
                'MAP { $bool as bool_val, $int as int_val, $str as str_val, $float as float_val }',
                {
                    bool: false,              // primitive boolean
                    int: new Int4Value(999),  // Value object
                    str: "mixed test",        // primitive string
                    float: 2.718             // primitive float
                },
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            
            const result = frames[0][0];
            expect(result.bool_val instanceof BoolValue).toBe(true);
            expect(result.bool_val.value).toBe(false);
            expect(result.int_val instanceof Int4Value).toBe(true);
            expect(result.int_val.value).toBe(999);
            expect(result.str_val instanceof Utf8Value).toBe(true);
            expect(result.str_val.value).toBe("mixed test");
            expect(result.float_val instanceof Float8Value).toBe(true);
            expect(result.float_val.value).toBeCloseTo(2.718);
        }, 1000);

        it('should accept primitive array parameters', async () => {
            const frames = await wsClient.command<[{ 
                bool_result: BoolValue,
                int_result: Int1Value,
                str_result: Utf8Value
            }]>(
                'MAP { $1 as bool_result, $2 as int_result, $3 as str_result }',
                [true, 123, "array test"], // primitive array
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            
            const result = frames[0][0];
            expect(result.bool_result instanceof BoolValue).toBe(true);
            expect(result.bool_result.value).toBe(true);
            expect(result.int_result instanceof Int1Value).toBe(true);
            expect(result.int_result.value).toBe(123);
            expect(result.str_result instanceof Utf8Value).toBe(true);
            expect(result.str_result.value).toBe("array test");
        }, 1000);
    });

    describe('query with primitive parameters', () => {
        it('should accept primitive boolean and return BoolValue', async () => {
            const frames = await wsClient.query<[{ result: BoolValue }]>(
                'MAP $value as result',
                { value: false }, // primitive boolean
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof BoolValue).toBe(true);
            expect(frames[0][0].result.value).toBe(false);
        }, 1000);

        it('should accept primitive integer and return appropriate IntValue', async () => {
            const frames = await wsClient.query<[{ result: Int4Value }]>(
                'MAP $num as result',
                { num: 50000 }, // primitive number
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Int4Value).toBe(true);
            expect(frames[0][0].result.value).toBe(50000);
        }, 1000);

        it('should accept primitive float and return Float8Value', async () => {
            const frames = await wsClient.query<[{ result: Float8Value }]>(
                'MAP $pi as result',
                { pi: Math.PI }, // primitive float
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Float8Value).toBe(true);
            expect(frames[0][0].result.value).toBeCloseTo(Math.PI);
        }, 1000);

        it('should accept primitive string and return Utf8Value', async () => {
            const frames = await wsClient.query<[{ result: Utf8Value }]>(
                'MAP $text as result',
                { text: "query primitive test" }, // primitive string
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof Utf8Value).toBe(true);
            expect(frames[0][0].result.value).toBe("query primitive test");
        }, 1000);

        it('should accept primitive Date and return DateTimeValue', async () => {
            const testDate = new Date('2024-06-15T08:30:00Z');
            const frames = await wsClient.query<[{ result: DateTimeValue }]>(
                'MAP $date as result',
                { date: testDate }, // primitive Date
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result instanceof DateTimeValue).toBe(true);
            expect(frames[0][0].result.value).toEqual(testDate);
        }, 1000);

        it('should accept mixed primitive and Value object parameters', async () => {
            const frames = await wsClient.query<[{
                mixed_bool: BoolValue,
                mixed_int: Int4Value,
                mixed_str: Utf8Value
            }]>(
                'MAP { $bool_param as mixed_bool, $int_param as mixed_int, $str_param as mixed_str }',
                {
                    bool_param: true,                    // primitive
                    int_param: new Int4Value(777),       // Value object  
                    str_param: "query mixed test"        // primitive
                },
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            
            const result = frames[0][0];
            expect(result.mixed_bool instanceof BoolValue).toBe(true);
            expect(result.mixed_bool.value).toBe(true);
            expect(result.mixed_int instanceof Int4Value).toBe(true);
            expect(result.mixed_int.value).toBe(777);
            expect(result.mixed_str instanceof Utf8Value).toBe(true);
            expect(result.mixed_str.value).toBe("query mixed test");
        }, 1000);

        it('should accept primitive array parameters', async () => {
            const frames = await wsClient.query<[{
                first: BoolValue,
                second: Float8Value,
                third: Utf8Value
            }]>(
                'MAP { $1 as first, $2 as second, $3 as third }',
                [false, 42.5, "query array"], // primitive array
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            
            const result = frames[0][0];
            expect(result.first instanceof BoolValue).toBe(true);
            expect(result.first.value).toBe(false);
            expect(result.second instanceof Float8Value).toBe(true);
            expect(result.second.value).toBe(42.5);
            expect(result.third instanceof Utf8Value).toBe(true);
            expect(result.third.value).toBe("query array");
        }, 1000);

        it('should handle complex primitive parameter scenarios', async () => {
            const frames = await wsClient.query<[{
                small_int: Int1Value,
                large_int: Int8Value,
                precise_float: Float8Value,
                text_field: Utf8Value,
                is_active: BoolValue
            }]>(
                'MAP { $small as small_int, $large as large_int, $precise as precise_float, $text as text_field, $active as is_active }',
                {
                    small: 100,                        // primitive -> Int1Value
                    large: 5000000000,                 // primitive -> Int8Value  
                    precise: 123.456789,               // primitive -> Float8Value
                    text: "complex test scenario",     // primitive -> Utf8Value
                    active: true                       // primitive -> BoolValue
                },
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            
            const result = frames[0][0];
            expect(result.small_int instanceof Int1Value).toBe(true);
            expect(result.small_int.value).toBe(100);
            expect(result.large_int instanceof Int8Value).toBe(true);
            expect(result.large_int.value).toBe(BigInt(5000000000));
            expect(result.precise_float instanceof Float8Value).toBe(true);
            expect(result.precise_float.value).toBeCloseTo(123.456789);
            expect(result.text_field instanceof Utf8Value).toBe(true);
            expect(result.text_field.value).toBe("complex test scenario");
            expect(result.is_active instanceof BoolValue).toBe(true);
            expect(result.is_active.value).toBe(true);
        }, 1000);
    });
});