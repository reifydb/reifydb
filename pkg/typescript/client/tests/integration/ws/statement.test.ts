// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Schema} from "@reifydb/core";


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

    describe('admin', () => {

        it('no statements', async () => {
            const frames = await wsClient.admin(
                '',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.admin(
                ';',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.admin(
                ';;;;;',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.admin(
                ';OUTPUT MAP 1 as one ;;;MAP 2 as two',
                {},
                [
                    Schema.object({one: Schema.int4Value()}),
                    Schema.object({two: Schema.int4Value()})
                ]
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one.value).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two.value).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.admin(
                'MAP 1 as result;',
                {},
                [Schema.object({result: Schema.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.admin(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP 2 as result;' +
                'MAP 3 as result;',
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);
            expect(frames[1][0].result.value).toBe(2);
            expect(frames[2][0].result.value).toBe(3);
        }, 1000);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.admin(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP { 2 as a, 3 as b };' +
                "MAP 'ReifyDB' as result;",
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({a: Schema.int4Value(), b: Schema.int4Value()}),
                    Schema.object({result: Schema.utf8Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);

            expect(frames[1][0].a.value).toBe(2);
            expect(frames[1][0].b.value).toBe(3);

            expect(frames[2][0].result.value).toBe("ReifyDB");
        }, 1000);
    });

    describe('command', () => {

        it('no statements', async () => {
            const frames = await wsClient.command(
                '',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.command(
                ';',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.command(
                ';;;;;',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.command(
                ';OUTPUT MAP 1 as one ;;;MAP 2 as two',
                {},
                [
                    Schema.object({one: Schema.int4Value()}),
                    Schema.object({two: Schema.int4Value()})
                ]
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one.value).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two.value).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.command(
                'MAP 1 as result;',
                {},
                [Schema.object({result: Schema.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP 2 as result;' +
                'MAP 3 as result;',
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);
            expect(frames[1][0].result.value).toBe(2);
            expect(frames[2][0].result.value).toBe(3);
        }, 1000);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP { 2 as a, 3 as b };' +
                "MAP 'ReifyDB' as result;",
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({a: Schema.int4Value(), b: Schema.int4Value()}),
                    Schema.object({result: Schema.utf8Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);

            expect(frames[1][0].a.value).toBe(2);
            expect(frames[1][0].b.value).toBe(3);

            expect(frames[2][0].result.value).toBe("ReifyDB");
        }, 1000);
    });


    describe('query', () => {

        it('no statements', async () => {
            const frames = await wsClient.query(
                '',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.query(
                ';',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.query(
                ';;;;;',
                {},
                []
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.query(
                ';OUTPUT MAP 1 as one ;;;MAP 2 as two',
                {},
                [
                    Schema.object({one: Schema.int4Value()}),
                    Schema.object({two: Schema.int4Value()})
                ]
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one.value).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two.value).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.query(
                'MAP 1 as result;',
                {},
                [Schema.object({result: Schema.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.query(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP 2 as result;' +
                'MAP 3 as result;',
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({result: Schema.int4Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);
            expect(frames[1][0].result.value).toBe(2);
            expect(frames[2][0].result.value).toBe(3);
        }, 1000);

        it('multiple statements, different structure', async () => {
            const frames = await wsClient.query(
                'OUTPUT MAP 1 as result;' +
                'OUTPUT MAP { 2 as a, 3 as b } ;' +
                "MAP 'ReifyDB' as result;",
                {},
                [
                    Schema.object({result: Schema.int4Value()}),
                    Schema.object({a: Schema.int4Value(), b: Schema.int4Value()}),
                    Schema.object({result: Schema.utf8Value()})
                ]
            );

            expect(frames).toHaveLength(3);

            expect(frames[0]).toHaveLength(1);
            expect(frames[1]).toHaveLength(1);
            expect(frames[2]).toHaveLength(1);

            expect(frames[0][0].result.value).toBe(1);

            expect(frames[1][0].a.value).toBe(2);
            expect(frames[1][0].b.value).toBe(3);

            expect(frames[2][0].result.value).toBe("ReifyDB");
        }, 1000);

    });


});
