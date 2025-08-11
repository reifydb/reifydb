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
    Int16Value,
    Uint1Value,
    Uint2Value,
    Uint4Value,
    Uint8Value,
    Uint16Value,
    Float4Value,
    Float8Value,
    Utf8Value,
    DateValue,
    DateTimeValue,
    TimeValue,
    IntervalValue,
    Uuid4Value,
    Uuid7Value,
    UndefinedValue
} from "../../../src";

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
            const frames = await wsClient.command<[{ result: BoolValue }]>(
                'MAP $1 as result',
                [new BoolValue(true)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(true);
            expect(typeof frames[0][0].result.value).toBe('boolean');
        }, 1000);

        it('Int4 type', async () => {
            const frames = await wsClient.command<[{ result: Int4Value }]>(
                'MAP $1 as result',
                [new Int4Value(42)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(42);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Int8 type with bigint', async () => {
            const frames = await wsClient.command<[{ result: Int8Value }]>(
                'MAP $1 as result',
                [new Int8Value(BigInt("9223372036854775807"))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt("9223372036854775807"));
            expect(typeof frames[0][0].result.value).toBe('bigint');
        }, 1000);

        it('Float8 type', async () => {
            const frames = await wsClient.command<[{ result: Float8Value }]>(
                'MAP $1 as result',
                [new Float8Value(3.14159)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(3.14159);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Utf8 type', async () => {
            const frames = await wsClient.command<[{ result: Utf8Value }]>(
                'MAP $1 as result',
                [new Utf8Value("Hello, ReifyDB!")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe("Hello, ReifyDB!");
            expect(typeof frames[0][0].result.value).toBe('string');
        }, 1000);

        it('DateTime type', async () => {
            const testDate = new Date('2024-01-15T10:30:00Z');
            const frames = await wsClient.command<[{ result: DateTimeValue }]>(
                'MAP $1 as result',
                [new DateTimeValue(testDate)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(testDate);
            expect(frames[0][0].result.value instanceof Date).toBe(true);
        }, 1000);

        it('Undefined type', async () => {
            const frames = await wsClient.command<[{ result: UndefinedValue }]>(
                'MAP $1 as result',
                [new UndefinedValue()]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(undefined);
            expect(typeof frames[0][0].result.value).toBe('undefined');
        }, 1000);

        it('Time type', async () => {
            const frames = await wsClient.command<[{ result: TimeValue }]>(
                'MAP $1 as result',
                [new TimeValue("15:45:30.250000000")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof TimeValue).toBe(true);
            expect(result.hour()).toBe(15);
            expect(result.minute()).toBe(45);
            expect(result.second()).toBe(30);
            expect(result.nanosecond()).toBe(250000000);
        }, 1000);

        it('Interval type', async () => {
            const interval = new IntervalValue({
                months: 0,
                days: 2,
                nanos: BigInt(3 * 3600 + 15 * 60) * BigInt(1_000_000_000)
            });
            const frames = await wsClient.command<[{ result: IntervalValue }]>(
                'MAP $1 as result',
                [interval]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof IntervalValue).toBe(true);
            // Check components separately: 0 months, 2 days, and time component nanos
            expect(result.getMonths()).toBe(0);
            expect(result.getDays()).toBe(2);
            expect(result.nanoseconds()).toBe(BigInt(11700000000000)); // 3 hours + 15 minutes in nanos
        }, 1000);

        it('Int1 type', async () => {
            const frames = await wsClient.command<[{ result: Int1Value }]>(
                'MAP $1 as result',
                [new Int1Value(127)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(127);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Int2 type', async () => {
            const frames = await wsClient.command<[{ result: Int2Value }]>(
                'MAP $1 as result',
                [new Int2Value(32767)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(32767);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Uint1 type', async () => {
            const frames = await wsClient.command<[{ result: Uint1Value }]>(
                'MAP $1 as result',
                [new Uint1Value(255)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(255);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Uint2 type', async () => {
            const frames = await wsClient.command<[{ result: Uint2Value }]>(
                'MAP $1 as result',
                [new Uint2Value(65535)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(65535);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Uint4 type', async () => {
            const frames = await wsClient.command<[{ result: Uint4Value }]>(
                'MAP $1 as result',
                [new Uint4Value(4294967295)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(4294967295);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Multiple parameters of different types', async () => {
            const frames = await wsClient.command<[{
                bool_val: BoolValue,
                int_val: Int4Value,
                str_val: Utf8Value
            }]>(
                'MAP { $1 as bool_val, $2 as int_val, $3 as str_val }',
                [new BoolValue(true), new Int4Value(123), new Utf8Value("test")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val.value).toBe(true);
            expect(frames[0][0].int_val.value).toBe(123);
            expect(frames[0][0].str_val.value).toBe("test");
        }, 1000);

        it('Auto-inferred primitive types', async () => {
            const frames = await wsClient.command<[{
                bool_val: BoolValue,
                num_val: Float8Value,
                str_val: Utf8Value
            }]>(
                'MAP { $1 as bool_val, $2 as num_val, $3 as str_val }',
                [new BoolValue(true), new Float8Value(42.5), new Utf8Value("auto-inferred")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].bool_val.value).toBe(true);
            expect(frames[0][0].num_val.value).toBe(42.5);
            expect(frames[0][0].str_val.value).toBe("auto-inferred");
        }, 1000);
    });

    describe('query', () => {
        it('Bool type', async () => {
            const frames = await wsClient.query<[{ result: BoolValue }]>(
                'MAP $1 as result',
                [new BoolValue(false)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(false);
            expect(typeof frames[0][0].result.value).toBe('boolean');
        }, 1000);

        it('Int4 type', async () => {
            const frames = await wsClient.query<[{ result: Int4Value }]>(
                'MAP $1 as result',
                [new Int4Value(-42)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(-42);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Float4 type', async () => {
            const frames = await wsClient.query<[{ result: Float4Value }]>(
                'MAP $1 as result',
                [new Float4Value(2.71828)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(2.71828, 4);
            expect(typeof frames[0][0].result.value).toBe('number');
        }, 1000);

        it('Uuid7 type', async () => {
            const uuid = "018fad5d-f37a-7c94-a716-446655440000";
            const frames = await wsClient.query<[{ result: Uuid7Value }]>(
                'MAP $1 as result',
                [new Uuid7Value(uuid)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(uuid);
            expect(typeof frames[0][0].result.value).toBe('string');
        }, 1000);

        it('Date type', async () => {
            const testDate = new Date('2024-01-15');
            const frames = await wsClient.query<[{ result: DateValue }]>(
                'MAP $1 as result',
                [new DateValue(testDate)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(testDate);
            expect(frames[0][0].result.value instanceof Date).toBe(true);
        }, 1000);

        it('Time type', async () => {
            const testTime = new Date('1970-01-01T09:15:45.500Z');
            const frames = await wsClient.query<[{ result: TimeValue }]>(
                'MAP $1 as result',
                [new TimeValue("09:15:45.500000000")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof TimeValue).toBe(true);
            expect(result.hour()).toBe(9);
            expect(result.minute()).toBe(15);
            expect(result.second()).toBe(45);
            expect(result.nanosecond()).toBe(500000000);
        }, 1000);

        it('Interval type', async () => {
            const interval = new IntervalValue({
                months: 0,
                days: 0,
                nanos: BigInt(4 * 3600 + 30 * 60 + 15) * BigInt(1_000_000_000)
            });
            const frames = await wsClient.query<[{ result: IntervalValue }]>(
                'MAP $1 as result',
                [interval]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof IntervalValue).toBe(true);
            const expectedNanos = BigInt((4 * 60 * 60 + 30 * 60 + 15) * 1_000_000_000);
            expect(result.nanoseconds()).toBe(expectedNanos);
        }, 1000);

        it('Uuid4 type', async () => {
            const uuid = '550e8400-e29b-41d4-a716-446655440000';
            const frames = await wsClient.query<[{ result: Uuid4Value }]>(
                'MAP $1 as result',
                [new Uuid4Value(uuid)]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(uuid);
            expect(typeof frames[0][0].result.value).toBe('string');
        }, 1000);

        it('Int16 type', async () => {
            const frames = await wsClient.query<[{ result: Int16Value }]>(
                'MAP $1 as result',
                [new Int16Value(BigInt('123456789012345'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt('123456789012345'));
            expect(typeof frames[0][0].result.value).toBe('bigint');
        }, 1000);

        it('Uint8 type', async () => {
            const frames = await wsClient.query<[{ result: Uint8Value }]>(
                'MAP $1 as result',
                [new Uint8Value(BigInt('18446744073709551615'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt('18446744073709551615'));
            expect(typeof frames[0][0].result.value).toBe('bigint');
        }, 1000);

        it('Uint16 type', async () => {
            const frames = await wsClient.query<[{ result: Uint16Value }]>(
                'MAP $1 as result',
                [new Uint16Value(BigInt('340282366920938463463374607431768211455'))]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt('340282366920938463463374607431768211455'));
            expect(typeof frames[0][0].result.value).toBe('bigint');
        }, 1000);

        it('Mixed explicit and auto-inferred types', async () => {
            const frames = await wsClient.query<[{
                explicit_int: Int2Value,
                auto_bool: BoolValue,
                explicit_str: Utf8Value
            }]>(
                'MAP { $1 as explicit_int, $2 as auto_bool, $3 as explicit_str }',
                [new Int2Value(32767), new BoolValue(false), new Utf8Value("explicit")]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].explicit_int.value).toBe(32767);
            expect(frames[0][0].auto_bool.value).toBe(false);
            expect(frames[0][0].explicit_str.value).toBe("explicit");
        }, 1000);
    });
});