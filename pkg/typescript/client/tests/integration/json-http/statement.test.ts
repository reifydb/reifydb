// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {beforeAll, describe, expect, it} from 'vitest';
import {Client, JsonHttpClient} from "../../../src";


describe('Statement', () => {
    let httpClient: JsonHttpClient;

    beforeAll(async () => {
        httpClient = Client.connect_json_http(process.env.REIFYDB_HTTP_URL, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    });

    describe('admin', () => {

        it('no statements', async () => {
            const frames = await httpClient.admin('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await httpClient.admin(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await httpClient.admin(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await httpClient.admin(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await httpClient.admin('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await httpClient.admin(
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
            const frames = await httpClient.admin(
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
            const frames = await httpClient.command('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await httpClient.command(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await httpClient.command(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await httpClient.command(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await httpClient.command('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await httpClient.command(
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
            const frames = await httpClient.command(
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
            const frames = await httpClient.query('');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await httpClient.query(';');
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await httpClient.query(';;;;;');
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await httpClient.query(
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}'
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await httpClient.query('MAP {result: 1};');

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await httpClient.query(
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
            const frames = await httpClient.query(
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
