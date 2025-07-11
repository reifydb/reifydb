/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";

describe('Statement', () => {
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

        it('no statements', async () => {
            const frames = await wsClient.tx<[{}]>(
                ''
            );
            expect(frames).toHaveLength(0);
        }, 10);

        it('single empty statement', async () => {
            const frames = await wsClient.tx<[{}]>(
                ';'
            );
            expect(frames).toHaveLength(0);
        }, 10);


        it('many empty statement', async () => {
            const frames = await wsClient.tx<[{}]>(
                ';;;;;'
            );
            expect(frames).toHaveLength(0);
        }, 10);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.tx<[
                { one: number },
                { two: number }
            ]>(
                ';SELECT 1 as one ;;;Select 2 as two'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 10);

        it('single statement', async () => {
            const frames = await wsClient.tx<[{ result: boolean }]>(
                'SELECT 1 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 10);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.tx<[
                { result: number },
                { result: number },
                { result: number },
            ]>(
                'SELECT 1 as result;' +
                'SELECT 2 as result;' +
                'SELECT 3 as result;'
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);
            expect(frames[1][0].result).toBe(2);
            expect(frames[2][0].result).toBe(3);
        }, 10);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.tx<[
                { result: number },
                { a: number, b: number },
                { result: string },
            ]>(
                'SELECT 1 as result;' +
                'SELECT 2 as a, 3 as b;' +
                "SELECT 'ReifyDB' as result;"
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);

            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);

            expect(frames[2][0].result).toBe("ReifyDB");
        }, 10);
    });


    describe('rx', () => {

        it('no statements', async () => {
            const frames = await wsClient.rx<[{}]>(
                ''
            );
            expect(frames).toHaveLength(0);
        }, 10);

        it('single empty statement', async () => {
            const frames = await wsClient.rx<[{}]>(
                ';'
            );
            expect(frames).toHaveLength(0);
        }, 10);


        it('many empty statement', async () => {
            const frames = await wsClient.rx<[{}]>(
                ';;;;;'
            );
            expect(frames).toHaveLength(0);
        }, 10);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.rx<[
                { one: number },
                { two: number }
            ]>(
                ';SELECT 1 as one ;;;Select 2 as two'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 10);

        it('single statement', async () => {
            const frames = await wsClient.rx<[{ result: boolean }]>(
                'SELECT 1 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 10);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.rx<[
                { result: number },
                { result: number },
                { result: number },
            ]>(
                'SELECT 1 as result;' +
                'SELECT 2 as result;' +
                'SELECT 3 as result;'
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);
            expect(frames[1][0].result).toBe(2);
            expect(frames[2][0].result).toBe(3);
        }, 10);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.rx<[
                { result: number },
                { a: number, b: number },
                { result: string },
            ]>(
                'SELECT 1 as result;' +
                'SELECT 2 as a, 3 as b;' +
                "SELECT 'ReifyDB' as result;"
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);

            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);

            expect(frames[2][0].result).toBe("ReifyDB");
        }, 10);

    });


});
