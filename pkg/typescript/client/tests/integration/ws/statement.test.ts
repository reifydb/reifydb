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

    describe('command', () => {

        it('no statements', async () => {
            const frames = await wsClient.command<[{}]>(
                ''
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.command<[{}]>(
                ';'
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.command<[{}]>(
                ';;;;;'
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.command<[
                { one: number },
                { two: number }
            ]>(
                ';MAP 1 as one ;;;MAP 2 as two'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.command<[{ result: boolean }]>(
                'MAP 1 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.command<[
                { result: number },
                { result: number },
                { result: number },
            ]>(
                'MAP 1 as result;' +
                'MAP 2 as result;' +
                'MAP 3 as result;'
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);
            expect(frames[1][0].result).toBe(2);
            expect(frames[2][0].result).toBe(3);
        }, 1000);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.command<[
                { result: number },
                { a: number, b: number },
                { result: string },
            ]>(
                'MAP 1 as result;' +
                'MAP { 2 as a, 3 as b };' +
                "MAP 'ReifyDB' as result;"
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);

            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);

            expect(frames[2][0].result).toBe("ReifyDB");
        }, 1000);
    });


    describe('query', () => {

        it('no statements', async () => {
            const frames = await wsClient.query<[{}]>(
                ''
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.query<[{}]>(
                ';'
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.query<[{}]>(
                ';;;;;'
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.query<[
                { one: number },
                { two: number }
            ]>(
                ';MAP 1 as one ;;;MAP 2 as two'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.query<[{ result: boolean }]>(
                'MAP 1 as result;'
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.query<[
                { result: number },
                { result: number },
                { result: number },
            ]>(
                'MAP 1 as result;' +
                'MAP 2 as result;' +
                'MAP 3 as result;'
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);
            expect(frames[1][0].result).toBe(2);
            expect(frames[2][0].result).toBe(3);
        }, 1000);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.query<[
                { result: number },
                { a: number, b: number },
                { result: string },
            ]>(
                'MAP 1 as result;' +
                'MAP { 2 as a, 3 as b } ;' +
                "MAP 'ReifyDB' as result;"
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result).toBe(1);

            expect(frames[1][0].a).toBe(2);
            expect(frames[1][0].b).toBe(3);

            expect(frames[2][0].result).toBe("ReifyDB");
        }, 1000);

    });


});
