// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {wait_for_database} from "../setup";
import {Client, JsonWsClient} from "../../../src";


describe('Statement', () => {
    let ws_client: JsonWsClient;

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);


    beforeEach(async () => {
        try {
            ws_client = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN,
            });
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            throw error;
        }
    }, 15000);


    afterEach(async () => {
        if (ws_client) {
            try {
                ws_client.disconnect();
            } catch (error) {
                console.error('Error during disconnect:', error);
            }
            ws_client = null;
        }
    });

    describe('admin', () => {

        it('no statements', async () => {
            const frames = await ws_client.admin('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await ws_client.admin(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await ws_client.admin(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await ws_client.admin(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await ws_client.admin('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await ws_client.admin(
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
            const frames = await ws_client.admin(
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
            const frames = await ws_client.command('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await ws_client.command(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await ws_client.command(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await ws_client.command(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await ws_client.command('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await ws_client.command(
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
            const frames = await ws_client.command(
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
            const frames = await ws_client.query('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await ws_client.query(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await ws_client.query(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await ws_client.query(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await ws_client.query('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await ws_client.query(
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
            const frames = await ws_client.query(
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
