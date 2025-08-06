/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, Interval, Value, WsClient} from "../../../src";

describe('Positional Parameters', () => {
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
                'MAP $1 as result',
                [Value.Bool(true)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
            expect(typeof frames[0][0].result).toBe('boolean');
        }, 1000);

        it('Int4 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Int4(42)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Int8 type with bigint', async () => {
            const frames = await wsClient.command<[{ result: bigint }]>(
                'MAP $1 as result',
                [Value.Int8(BigInt("9223372036854775807"))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt("9223372036854775807"));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Float8 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Float8(3.14159)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.14159);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Utf8 type', async () => {
            const frames = await wsClient.command<[{ result: string }]>(
                'MAP $1 as result',
                [Value.Utf8("Hello, ReifyDB!")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe("Hello, ReifyDB!");
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('DateTime type', async () => {
            const testDate = new Date('2024-01-15T10:30:00Z');
            const frames = await wsClient.command<[{ result: Date }]>(
                'MAP $1 as result',
                [Value.DateTime(testDate)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(testDate);
            expect(frames[0][0].result instanceof Date).toBe(true);
        }, 1000);

        it('Undefined type', async () => {
            const frames = await wsClient.command<[{ result: undefined }]>(
                'MAP $1 as result',
                [Value.Undefined()]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(undefined);
            expect(typeof frames[0][0].result).toBe('undefined');
        }, 1000);

        it('Time type', async () => {
            const testTime = new Date('1970-01-01T15:45:30.250Z');
            const frames = await wsClient.command<[{ result: Date }]>(
                'MAP $1 as result',
                [Value.Time(testTime)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Date).toBe(true);
            expect(result.getUTCHours()).toBe(15);
            expect(result.getUTCMinutes()).toBe(45);
            expect(result.getUTCSeconds()).toBe(30);
            expect(result.getUTCMilliseconds()).toBe(250);
        }, 1000);

        it('Interval type', async () => {
            const interval = Interval.from({days: 2, hours: 3, minutes: 15});
            const frames = await wsClient.command<[{ result: Interval }]>(
                'MAP $1 as result',
                [Value.Interval(interval)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Interval).toBe(true);
            // 2 days + 3 hours + 15 minutes
            const expectedNanos = BigInt((2 * 24 * 60 * 60 + 3 * 60 * 60 + 15 * 60) * 1_000_000_000);
            expect(result.totalNanoseconds).toBe(expectedNanos);
        }, 1000);

        it('Int1 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Int1(127)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(127);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Int2 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Int2(32767)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(32767);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uint1 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Uint1(255)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(255);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uint2 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Uint2(65535)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(65535);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uint4 type', async () => {
            const frames = await wsClient.command<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Uint4(4294967295)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(4294967295);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Multiple parameters of different types', async () => {
            const frames = await wsClient.command<[{
                bool_val: boolean,
                int_val: number,
                str_val: string
            }]>(
                'MAP { $1 as bool_val, $2 as int_val, $3 as str_val }',
                [Value.Bool(true), Value.Int4(123), Value.Utf8("test")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val).toBe(true);
            expect(frames[0][0].int_val).toBe(123);
            expect(frames[0][0].str_val).toBe("test");
        }, 1000);

        it('Auto-inferred primitive types', async () => {
            const frames = await wsClient.command<[{
                bool_val: boolean,
                num_val: number,
                str_val: string
            }]>(
                'MAP { $1 as bool_val, $2 as num_val, $3 as str_val }',
                [true, 42.5, "auto-inferred"]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val).toBe(true);
            expect(frames[0][0].num_val).toBe(42.5);
            expect(frames[0][0].str_val).toBe("auto-inferred");
        }, 1000);
    });

    describe('query', () => {
        it('Bool type', async () => {
            const frames = await wsClient.query<[{ result: boolean }]>(
                'MAP $1 as result',
                [Value.Bool(false)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(false);
            expect(typeof frames[0][0].result).toBe('boolean');
        }, 1000);

        it('Int4 type', async () => {
            const frames = await wsClient.query<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Int4(-42)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(-42);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Float4 type', async () => {
            const frames = await wsClient.query<[{ result: number }]>(
                'MAP $1 as result',
                [Value.Float4(2.71828)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(2.71828, 4);
            expect(typeof frames[0][0].result).toBe('number');
        }, 1000);

        it('Uuid7 type', async () => {
            const uuid = "01234567-89ab-7def-0123-456789abcdef";
            const frames = await wsClient.query<[{ result: string }]>(
                'MAP $1 as result',
                [Value.Uuid7(uuid)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(uuid);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Date type', async () => {
            const testDate = new Date('2024-01-15');
            const frames = await wsClient.query<[{ result: Date }]>(
                'MAP $1 as result',
                [Value.Date(testDate)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(testDate);
            expect(frames[0][0].result instanceof Date).toBe(true);
        }, 1000);

        it('Time type', async () => {
            const testTime = new Date('1970-01-01T09:15:45.500Z');
            const frames = await wsClient.query<[{ result: Date }]>(
                'MAP $1 as result',
                [Value.Time(testTime)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Date).toBe(true);
            expect(result.getUTCHours()).toBe(9);
            expect(result.getUTCMinutes()).toBe(15);
            expect(result.getUTCSeconds()).toBe(45);
            expect(result.getUTCMilliseconds()).toBe(500);
        }, 1000);

        it('Interval type', async () => {
            const interval = Interval.from({hours: 4, minutes: 30, seconds: 15});
            const frames = await wsClient.query<[{ result: Interval }]>(
                'MAP $duration as result',
                {duration: Value.Interval(interval)}
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof Interval).toBe(true);
            const expectedNanos = BigInt((4 * 60 * 60 + 30 * 60 + 15) * 1_000_000_000);
            expect(result.totalNanoseconds).toBe(expectedNanos);
        }, 1000);

        it('Uuid4 type', async () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const frames = await wsClient.query<[{ result: string }]>(
                'MAP $1 as result',
                [Value.Uuid4(uuid)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(uuid);
            expect(typeof frames[0][0].result).toBe('string');
        }, 1000);

        it('Int16 type', async () => {
            const frames = await wsClient.query<[{ result: bigint }]>(
                'MAP $1 as result',
                [Value.Int16(BigInt('123456789012345'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt('123456789012345'));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Uint8 type', async () => {
            const frames = await wsClient.query<[{ result: bigint }]>(
                'MAP $1 as result',
                [Value.Uint8(BigInt('18446744073709551615'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt('18446744073709551615'));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Uint16 type', async () => {
            const frames = await wsClient.query<[{ result: bigint }]>(
                'MAP $1 as result',
                [Value.Uint16(BigInt('340282366920938463463374607431768211455'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt('340282366920938463463374607431768211455'));
            expect(typeof frames[0][0].result).toBe('bigint');
        }, 1000);

        it('Mixed explicit and auto-inferred types', async () => {
            const frames = await wsClient.query<[{
                explicit_int: number,
                auto_bool: boolean,
                explicit_str: string
            }]>(
                'MAP { $1 as explicit_int, $2 as auto_bool, $3 as explicit_str }',
                [Value.Int2(32767), false, Value.Utf8("explicit")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].explicit_int).toBe(32767);
            expect(frames[0][0].auto_bool).toBe(false);
            expect(frames[0][0].explicit_str).toBe("explicit");
        }, 1000);
    });
});