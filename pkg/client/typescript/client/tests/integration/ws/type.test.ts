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
                'SELECT true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
        }, 10);

        it('float4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 10);

        it('float8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 10);

        it('int1', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);


        it('int2', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('int4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('int8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('int16', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('uint1', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);


        it('uint2', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('uint4', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('uint8', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('uint16', async () => {
            const frames = await wsClient.tx<[{ result: number }]>(
                'SELECT cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('text', async () => {
            const frames = await wsClient.tx<[{ result: string }]>(
                "SELECT cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe('Elodie');
        }, 10);
    });


    describe('rx', () => {

        it('boolean', async () => {
            const frames = await wsClient.rx<[{ result: boolean }]>(
                'SELECT true as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(true);
        }, 10);

        it('float4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(3.14, float4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 10);

        it('float8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(3.14, float8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(3.14);
        }, 10);

        it('int1', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, int1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);


        it('int2', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, int2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('int4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, int4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('int8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, int8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('int16', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, int16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('uint1', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, uint1) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);


        it('uint2', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, uint2) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('uint4', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, uint4) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(123);
        }, 10);

        it('uint8', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, uint8) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('uint16', async () => {
            const frames = await wsClient.rx<[{ result: number }]>(
                'SELECT cast(123, uint16) as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(BigInt(123));
        }, 10);

        it('text', async () => {
            const frames = await wsClient.rx<[{ result: string }]>(
                "SELECT cast('Elodie', text) as result;"
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe('Elodie');
        }, 10);
    });


});
