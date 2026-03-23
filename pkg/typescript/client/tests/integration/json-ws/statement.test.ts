// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWebsocketClient} from "../../../src";


describe('Statement', () => {
    let wsClient: JsonWebsocketClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);


    beforeEach(async () => {
        try {
            wsClient = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


    afterEach(async () => {
        if (wsClient) {
            try {
                wsClient.disconnect();
            } catch (error) {
                console.error('Error during disconnect:', error);
            }
            wsClient = null;
        }
    });

    describe('admin', () => {

        it('no statements', async () => {
            const frames = await wsClient.admin('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.admin(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.admin(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.admin(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.admin('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.admin(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};'
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
            const frames = await wsClient.admin(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};"
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

    describe('command', () => {

        it('no statements', async () => {
            const frames = await wsClient.command('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.command(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.command(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.command(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.command('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};'
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
            const frames = await wsClient.command(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};"
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
            const frames = await wsClient.query('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.query(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.query(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.query(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.query('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.query(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};'
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
            const frames = await wsClient.query(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};"
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
