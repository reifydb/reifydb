/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient, Value, Interval} from "../../../src";

describe('Named Parameters', () => {
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

    describe('command', () => {
        it('Bool type', async () => {
            const frames = await wsClient.command<[{ result: boolean }]>(
                'MAP $value as result',
                { value: Value.Bool(true) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
            expect(typeof frames[0][0].result).toBe('boolean');
        }, 1000);

        it('Int4 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $num as result',
                { num: Value.Int4(999) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(999);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Int16 type with bigint', async () => {
            const frames = await wsClient.command<[{ result: bigint }]>(
                'MAP $bignum as result',
                { bignum: Value.Int16(BigInt("12345678901234567890")) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt("12345678901234567890"));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Float8 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $pi as result',
                { pi: Value.Float8(Math.PI) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(Math.PI);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Utf8 type', async () => {
            const frames = await wsClient.command<[{ result: string }]>(
                'MAP $text as result',
                { text: Value.Utf8("Named parameter test") }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe("Named parameter test");
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('DateTime type', async () => {
            const testDate = new Date('2024-12-25T00:00:00Z');
            const frames = await wsClient.command<[{ result: Date }]>(
                'MAP $timestamp as result',
                { timestamp: Value.DateTime(testDate) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(testDate);
            expect(frames[0][0].result instanceof Date).toBe(true);
        }, 1000);

        it('Undefined type', async () => {
            const frames = await wsClient.command<[{ result: undefined }]>(
                'MAP $undef as result',
                { undef: Value.Undefined() }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(undefined);
            expect(typeof frames[0][0].result).toBe('undefined');
        }, 1000);

        it('Int1 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $tiny as result',
                { tiny: Value.Int1(-128) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(-128);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Int2 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $small as result',
                { small: Value.Int2(-32768) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(-32768);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Int8 type', async () => {
            const frames = await wsClient.command<[{ result: bigint }]>(
                'MAP $big as result',
                { big: Value.Int8(BigInt('-9223372036854775808')) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt('-9223372036854775808'));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Uint1 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $utiny as result',
                { utiny: Value.Uint1(200) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(200);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uint2 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $usmall as result',
                { usmall: Value.Uint2(50000) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(50000);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uuid7 type', async () => {
            const uuid = '018fad5d-f37a-7c94-a716-446655440000';
            const frames = await wsClient.command<[{ result: string }]>(
                'MAP $id7 as result',
                { id7: Value.Uuid7(uuid) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(uuid);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Interval type', async () => {
            const interval = Interval.from({ hours: 4, minutes: 30, seconds: 15 });
            const frames = await wsClient.command<[{ result: Interval }]>(
                'MAP $duration as result',
                { duration: Value.Interval(interval) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Interval).toBe(true);
            const expectedNanos = BigInt((4 * 60 * 60 + 30 * 60 + 15) * 1_000_000_000);
            expect(result.totalNanoseconds).toBe(expectedNanos);
        }, 1000);

        it('Multiple named parameters of different types', async () => {
            const frames = await wsClient.command<[{ 
                bool_val: boolean,
                int_val: number,
                str_val: string 
            }]>(
                'MAP { $is_active as bool_val, $count as int_val, $name as str_val }',
                {
                    is_active: Value.Bool(false),
                    count: Value.Int4(456),
                    name: Value.Utf8("ReifyDB")
                }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val).toBe(false);
            expect(frames[0][0].int_val).toBe(456);
            expect(frames[0][0].str_val).toBe("ReifyDB");
        }, 1000);

        it('Auto-inferred primitive types with named params', async () => {
            const frames = await wsClient.command<[{ 
                bool_val: boolean,
                num_val: number,
                str_val: string 
            }]>(
                'MAP { $flag as bool_val, $amount as num_val, $label as str_val }',
                {
                    flag: false,
                    amount: 99.99,
                    label: "auto-named"
                }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val).toBe(false);
            expect(frames[0][0].num_val).toBe(99.99);
            expect(frames[0][0].str_val).toBe("auto-named");
        }, 1000);
    });

    describe('query', () => {
        it('Bool type', async () => {
            const frames = await wsClient.query<[{ result: boolean }]>(
                'MAP $value as result',
                { value: Value.Bool(false) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(false);
            expect(typeof frames[0][0].result).toBe('boolean');
        }, 1000);

        it('Uint8 type', async () => {
            const frames = await wsClient.query<[{ result: bigint }]>(
                'MAP $unsigned as result',
                { unsigned: Value.Uint8(BigInt("18446744073709551615")) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt("18446744073709551615"));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Float4 type', async () => {
            const frames = await wsClient.query<[{ result: number }]>(
                'MAP $euler as result',
                { euler: Value.Float4(2.71828) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(2.71828, 4);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uuid4 type', async () => {
            const uuid = "550e8400-e29b-41d4-a716-446655440000";
            const frames = await wsClient.query<[{ result: string }]>(
                'MAP $id as result',
                { id: Value.Uuid4(uuid) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(uuid);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Date type', async () => {
            const testDate = new Date('2024-06-15');
            const frames = await wsClient.query<[{ result: Date }]>(
                'MAP $date as result',
                { date: Value.Date(testDate) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(testDate);
            expect(frames[0][0].result instanceof Date).toBe(true);
        }, 1000);

        it('Time type', async () => {
            const testTime = new Date('1970-01-01T14:30:00Z');
            const frames = await wsClient.query<[{ result: Date }]>(
                'MAP $time as result',
                { time: Value.Time(testTime) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Date).toBe(true);
            expect(result.getUTCHours()).toBe(14);
            expect(result.getUTCMinutes()).toBe(30);
            expect(result.getUTCSeconds()).toBe(0);
        }, 1000);

        it('Interval type', async () => {
            const interval = Interval.parse('P3Y6M4DT12H30M5S');
            const frames = await wsClient.query<[{ result: Interval }]>(
                'MAP $period as result',
                { period: Value.Interval(interval) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Interval).toBe(true);
            // 3 years (approx) + 6 months (approx) + 4 days + 12.5 hours + 5 seconds
            // Using approximations: 1 year = 365 days, 1 month = 30 days
            const expectedNanos = BigInt(
                ((3 * 365 + 6 * 30 + 4) * 24 * 60 * 60 + 12 * 60 * 60 + 30 * 60 + 5) * 1_000_000_000
            );
            expect(result.totalNanoseconds).toBe(expectedNanos);
        }, 1000);

        it('Uint16 type', async () => {
            const frames = await wsClient.query<[{ result: bigint }]>(
                'MAP $huge as result',
                { huge: Value.Uint16(BigInt('123456789012345678901234567890')) }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt('123456789012345678901234567890'));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Mixed explicit and auto-inferred types with named params', async () => {
            const frames = await wsClient.query<[{ 
                explicit_uint: number,
                auto_str: string,
                explicit_float: number 
            }]>(
                'MAP { $uint_val as explicit_uint, $str_val as auto_str, $float_val as explicit_float }',
                {
                    uint_val: Value.Uint4(65535),
                    str_val: "auto-inferred string",
                    float_val: Value.Float8(1.23456789)
                }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].explicit_uint).toBe(65535);
            expect(frames[0][0].auto_str).toBe("auto-inferred string");
            expect(frames[0][0].explicit_float).toBeCloseTo(1.23456789);
        }, 1000);

        it('Complex named parameters with various types', async () => {
            const testDate = new Date('2024-01-01T12:00:00Z');
            const frames = await wsClient.query<[{ 
                id: string,
                active: boolean,
                score: number,
                created: Date,
                name: string
            }]>(
                'MAP { $user_id as id, $is_active as active, $rating as score, $created_at as created, $username as name }',
                {
                    user_id: Value.Uuid7("01234567-89ab-7def-0123-456789abcdef"),
                    is_active: true,
                    rating: Value.Float8(4.95),
                    created_at: Value.DateTime(testDate),
                    username: "test_user"
                }
            );
            
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0];
            expect(result.id).toBe("01234567-89ab-7def-0123-456789abcdef");
            expect(result.active).toBe(true);
            expect(result.score).toBeCloseTo(4.95);
            expect(result.created).toEqual(testDate);
            expect(result.name).toBe("test_user");
        }, 1000);
    });
});