/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";

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

    describe('tx', () => {

        it('boolean', async () => {
            const frames = await wsClient.tx<[{ result: boolean }]>(
                'map true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
        }, 1000);

        it('float4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 1000);

        it('float8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 1000);

        it('int1', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);


        it('int2', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('int4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('int8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('int16', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('uint1', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);


        it('uint2', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('uint4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('uint8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('uint16', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'map cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('text', async () => {
            const frames = await wsClient.tx<[{ result: string }]>(
                "map cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe('Elodie');
        }, 1000);

        it('date', async () => {
            const frames = await wsClient.tx<[{ result: Date }]>(
                'map @2024-03-15 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(new Date('2024-03-15'));
        }, 1000);

        it('datetime', async () => {
            const frames = await wsClient.tx<[{ result: Date }]>(
                'map @2024-03-15T14:30:00.123456789Z as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(new Date('2024-03-15T14:30:00.123456789Z'));
        }, 1000);

        it('time', async () => {
            const frames = await wsClient.tx<[{ result: Date }]>(
                'map @14:30:00.123456789 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result).toBeInstanceOf(Date);
            expect(result.getHours()).toBe(14);
            expect(result.getMinutes()).toBe(30);
            expect(result.getSeconds()).toBe(0);
            expect(result.getMilliseconds()).toBe(123);
        }, 1000);

        it('interval', async () => {
            const frames = await wsClient.tx<[{ result: bigint }]>(
                'map @P1DT2H30M as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // 1 day + 2 hours + 30 minutes = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
            const expected = BigInt((24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000);
            expect(frames[0][0].result).toBe(expected);
        }, 1000);
    });


    describe('rx', () => {

        it('boolean', async () => {
            const frames = await wsClient.rx<[{ result: boolean }]>(
                'map true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
        }, 1000);

        it('float4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 1000);

        it('float8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 1000);

        it('int1', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);


        it('int2', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('int4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('int8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('int16', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('uint1', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);


        it('uint2', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('uint4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 1000);

        it('uint8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('uint16', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'map cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 1000);

        it('text', async () => {
            const frames = await wsClient.rx<[{ result: string }]>(
                "map cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe('Elodie');
        }, 1000);

        it('date', async () => {
            const frames = await wsClient.rx<[{ result: Date }]>(
                'map @2024-03-15 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(new Date('2024-03-15'));
        }, 1000);

        it('datetime', async () => {
            const frames = await wsClient.rx<[{ result: Date }]>(
                'map @2024-03-15T14:30:00.123456789Z as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toEqual(new Date('2024-03-15T14:30:00.123456789Z'));
        }, 1000);

        it('time', async () => {
            const frames = await wsClient.rx<[{ result: Date }]>(
                'map @14:30:00.123456789 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            const result = frames[0][0].result;
            expect(result).toBeInstanceOf(Date);
            expect(result.getHours()).toBe(14);
            expect(result.getMinutes()).toBe(30);
            expect(result.getSeconds()).toBe(0);
            expect(result.getMilliseconds()).toBe(123);
        }, 1000);

        it('interval', async () => {
            const frames = await wsClient.rx<[{ result: bigint }]>(
                'map @P1DT2H30M as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            // 1 day + 2 hours + 30 minutes = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
            const expected = BigInt((24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000);
            expect(frames[0][0].result).toBe(expected);
        }, 1000);
    });


});
