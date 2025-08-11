/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient, Int4Value, Utf8Value, Schema} from "../../../src";
import { LEGACY_SCHEMA } from "../test-helpers";

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
                '',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.command<[{}]>(
                ';',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.command<[{}]>(
                ';;;;;',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.command<[
                { one: Int4Value },
                { two: Int4Value }
            ]>(
                ';MAP 1 as one ;;;MAP 2 as two',
                LEGACY_SCHEMA // Returns Value objects
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one.value).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two.value).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.command<[{ result: Int4Value }]>(
                'MAP 1 as result;',
                LEGACY_SCHEMA // Returns Value objects
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.command<[
                { result: Int4Value },
                { result: Int4Value },
                { result: Int4Value },
            ]>(
                'MAP 1 as result;' +
                'MAP 2 as result;' +
                'MAP 3 as result;',
                LEGACY_SCHEMA
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
            const frames = await wsClient.command<[
                { result: Int4Value },
                { a: Int4Value, b: Int4Value },
                { result: Utf8Value },
            ]>(
                'MAP 1 as result;' +
                'MAP { 2 as a, 3 as b };' +
                "MAP 'ReifyDB' as result;",
                LEGACY_SCHEMA
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
            const frames = await wsClient.query<[{}]>(
                '',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('single empty statement', async () => {
            const frames = await wsClient.query<[{}]>(
                ';',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);


        it('many empty statement', async () => {
            const frames = await wsClient.query<[{}]>(
                ';;;;;',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(0);
        }, 1000);

        it('mixed empty and non empty', async () => {
            const frames = await wsClient.query<[
                { one: Int4Value },
                { two: Int4Value }
            ]>(
                ';MAP 1 as one ;;;MAP 2 as two',
                LEGACY_SCHEMA
            );
            expect(frames).toHaveLength(2);

            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].one.value).toBe(1);

            expect(frames[1]).toHaveLength(1);
            expect(frames[1][0].two.value).toBe(2);

        }, 1000);

        it('single statement', async () => {
            const frames = await wsClient.query<[{ result: Int4Value }]>(
                'MAP 1 as result;',
                LEGACY_SCHEMA
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.query<[
                { result: Int4Value },
                { result: Int4Value },
                { result: Int4Value },
            ]>(
                'MAP 1 as result;' +
                'MAP 2 as result;' +
                'MAP 3 as result;',
                LEGACY_SCHEMA
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
            const frames = await wsClient.query<[
                { result: Int4Value },
                { a: Int4Value, b: Int4Value },
                { result: Utf8Value },
            ]>(
                'MAP 1 as result;' +
                'MAP { 2 as a, 3 as b } ;' +
                "MAP 'ReifyDB' as result;",
                LEGACY_SCHEMA
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
