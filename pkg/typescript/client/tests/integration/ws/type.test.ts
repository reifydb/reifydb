/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {BoolValue, Client, Float4Value, Float8Value, Int1Value, Int2Value, Int4Value, Int8Value, Int16Value, Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value, Utf8Value, DateValue, DateTimeValue, IntervalValue, TimeValue, Uuid4Value, Uuid7Value} from "../../../src";
import {WsClient} from "../../../src/ws";

describe('Websocket Data Type', () => {
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

        it('boolean', async () => {
            const frames = await wsClient.command<[{ result: BoolValue }]>(
                'map true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(true);
        }, 1000);

        it('float4', async () => {
            const frames = await wsClient.command<[{ result: Float4Value }]>(
                'map cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(3.14, 4);
        }, 1000);

        it('float8', async () => {
            const frames = await wsClient.command<[{ result: Float8Value }]>(
                'map cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(3.14, 4);
        }, 1000);

        it('int1', async () => {
            const frames = await wsClient.command<[{ result: Int1Value }]>(
                'map cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);


        it('int2', async () => {
            const frames = await wsClient.command<[{ result: Int2Value }]>(
                'map cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('int4', async () => {
            const frames = await wsClient.command<[{ result: Int4Value }]>(
                'map cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('int8', async () => {
            const frames = await wsClient.command<[{ result: Int8Value }]>(
                'map cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('int16', async () => {
            const frames = await wsClient.command<[{ result: Int16Value }]>(
                'map cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('uint1', async () => {
            const frames = await wsClient.command<[{ result: Uint1Value }]>(
                'map cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);


        it('uint2', async () => {
            const frames = await wsClient.command<[{ result: Uint2Value }]>(
                'map cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('uint4', async () => {
            const frames = await wsClient.command<[{ result: Uint4Value }]>(
                'map cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('uint8', async () => {
            const frames = await wsClient.command<[{ result: Uint8Value }]>(
                'map cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('uint16', async () => {
            const frames = await wsClient.command<[{ result: Uint16Value }]>(
                'map cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('text', async () => {
            const frames = await wsClient.command<[{ result: Utf8Value }]>(
                "map cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('Elodie');
        }, 1000);

        it('date', async () => {
            const frames = await wsClient.command<[{ result: DateValue }]>(
                'map @2024-03-15 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(new Date('2024-03-15'));
        }, 1000);

        it('datetime', async () => {
            const frames = await wsClient.command<[{ result: DateTimeValue }]>(
                'map @2024-03-15T14:30:00.123456789Z as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(new Date('2024-03-15T14:30:00.123456789Z'));
        }, 1000);

        it('time', async () => {
            const frames = await wsClient.command<[{ result: TimeValue }]>(
                'map @14:30:00.123456789 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof TimeValue).toBe(true);
            expect(result.hour()).toBe(14);
            expect(result.minute()).toBe(30);
            expect(result.second()).toBe(0);
            expect(result.nanosecond()).toBe(123456789);
        }, 1000);

        it('interval', async () => {
            const frames = await wsClient.command<[{ result: IntervalValue }]>(
                'map @P1DT2H30M as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result).toBeInstanceOf(IntervalValue);
            // P1DT2H30M = 0 months, 1 day, 9000000000000 nanos (2.5 hours)
            expect(result.getMonths()).toBe(0);
            expect(result.getDays()).toBe(1);
            expect(result.nanoseconds()).toBe(BigInt(9000000000000)); // 2.5 hours in nanos
        }, 1000);

        it('uuid4', async () => {
            const frames = await wsClient.command<[{ result: Uuid4Value }]>(
                "map cast('550e8400-e29b-41d4-a716-446655440000', uuid4) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('550e8400-e29b-41d4-a716-446655440000');
        }, 1000);

        it('uuid7', async () => {
            const frames = await wsClient.command<[{ result: Uuid7Value }]>(
                "map cast('018fad5d-f37a-7c94-a716-446655440000', uuid7) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('018fad5d-f37a-7c94-a716-446655440000');
        }, 1000);
    });


    describe('query', () => {

        it('boolean', async () => {
            const frames = await wsClient.query<[{ result: BoolValue }]>(
                'map true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(true);
        }, 1000);

        it('float4', async () => {
            const frames = await wsClient.query<[{ result: Float4Value }]>(
                'map cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(3.14, 4);
        }, 1000);

        it('float8', async () => {
            const frames = await wsClient.query<[{ result: Float8Value }]>(
                'map cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBeCloseTo(3.14, 4);
        }, 1000);

        it('int1', async () => {
            const frames = await wsClient.query<[{ result: Int1Value }]>(
                'map cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);


        it('int2', async () => {
            const frames = await wsClient.query<[{ result: Int2Value }]>(
                'map cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('int4', async () => {
            const frames = await wsClient.query<[{ result: Int4Value }]>(
                'map cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('int8', async () => {
            const frames = await wsClient.query<[{ result: Int8Value }]>(
                'map cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('int16', async () => {
            const frames = await wsClient.query<[{ result: Int16Value }]>(
                'map cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('uint1', async () => {
            const frames = await wsClient.query<[{ result: Uint1Value }]>(
                'map cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);


        it('uint2', async () => {
            const frames = await wsClient.query<[{ result: Uint2Value }]>(
                'map cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('uint4', async () => {
            const frames = await wsClient.query<[{ result: Uint4Value }]>(
                'map cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(123);
        }, 1000);

        it('uint8', async () => {
            const frames = await wsClient.query<[{ result: Uint8Value }]>(
                'map cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('uint16', async () => {
            const frames = await wsClient.query<[{ result: Uint16Value }]>(
                'map cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(BigInt(123));
        }, 1000);

        it('text', async () => {
            const frames = await wsClient.query<[{ result: Utf8Value }]>(
                "map cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('Elodie');
        }, 1000);

        it('date', async () => {
            const frames = await wsClient.query<[{ result: DateValue }]>(
                'map @2024-03-15 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(new Date('2024-03-15'));
        }, 1000);

        it('datetime', async () => {
            const frames = await wsClient.query<[{ result: DateTimeValue }]>(
                'map @2024-03-15T14:30:00.123456789Z as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toEqual(new Date('2024-03-15T14:30:00.123456789Z'));
        }, 1000);

        it('time', async () => {
            const frames = await wsClient.query<[{ result: TimeValue }]>(
                'map @14:30:00.123456789 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result instanceof TimeValue).toBe(true);
            expect(result.hour()).toBe(14);
            expect(result.minute()).toBe(30);
            expect(result.second()).toBe(0);
            expect(result.nanosecond()).toBe(123456789);
        }, 1000);

        it('interval', async () => {
            const frames = await wsClient.query<[{ result: IntervalValue }]>(
                'map @P1DT2H30M as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result).toBeInstanceOf(IntervalValue);
            // P1DT2H30M = 0 months, 1 day, 9000000000000 nanos (2.5 hours)
            expect(result.getMonths()).toBe(0);
            expect(result.getDays()).toBe(1);
            expect(result.nanoseconds()).toBe(BigInt(9000000000000)); // 2.5 hours in nanos
        }, 1000);

        it('uuid4', async () => {
            const frames = await wsClient.query<[{ result: Uuid4Value }]>(
                "map cast('550e8400-e29b-41d4-a716-446655440000', uuid4) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('550e8400-e29b-41d4-a716-446655440000');
        }, 1000);

        it('uuid7', async () => {
            const frames = await wsClient.query<[{ result: Uuid7Value }]>(
                "map cast('018fad5d-f37a-7c94-a716-446655440000', uuid7) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe('018fad5d-f37a-7c94-a716-446655440000');
        }, 1000);
    });


});
