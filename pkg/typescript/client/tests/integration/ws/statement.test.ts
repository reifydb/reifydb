// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Shape} from "@reifydb/core";
import {assertMeta} from "../helpers";


describe.each([
    {format: "frames"},
    {format: "rbcf"},
] as const)('Statement [$format]', ({format}) => {
    let wsClient: WsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);


    beforeEach(async () => {
        try {
            wsClient = await Client.connectWs(process.env.REIFYDB_WS_URL, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN,
                format,
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
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}',
                {},
                [
                    Shape.object({one: Shape.int4Value()}),
                    Shape.object({two: Shape.int4Value()})
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
                'MAP {result: 1};',
                {},
                [Shape.object({result: Shape.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.admin(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};',
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()})
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
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};",
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({a: Shape.int4Value(), b: Shape.int4Value()}),
                    Shape.object({result: Shape.utf8Value()})
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
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}',
                {},
                [
                    Shape.object({one: Shape.int4Value()}),
                    Shape.object({two: Shape.int4Value()})
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
                'MAP {result: 1};',
                {},
                [Shape.object({result: Shape.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.command(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};',
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()})
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
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};",
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({a: Shape.int4Value(), b: Shape.int4Value()}),
                    Shape.object({result: Shape.utf8Value()})
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
                ';OUTPUT MAP {one: 1} ;;;MAP {two: 2}',
                {},
                [
                    Shape.object({one: Shape.int4Value()}),
                    Shape.object({two: Shape.int4Value()})
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
                'MAP {result: 1};',
                {},
                [Shape.object({result: Shape.int4Value()})]
            );

            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result.value).toBe(1);
        }, 1000);

        it('multiple statements, but same structure', async () => {
            const frames = await wsClient.query(
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP {result: 2};' +
                'MAP {result: 3};',
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({result: Shape.int4Value()})
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
                'OUTPUT MAP {result: 1};' +
                'OUTPUT MAP { a: 2, b: 3 };' +
                "MAP {result: 'ReifyDB'};",
                {},
                [
                    Shape.object({result: Shape.int4Value()}),
                    Shape.object({a: Shape.int4Value(), b: Shape.int4Value()}),
                    Shape.object({result: Shape.utf8Value()})
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

    describe('with_meta', () => {
        it('admin', async () => {
            const { frames, meta } = await wsClient.adminWithMeta(';', {}, []);
            expect(frames).toHaveLength(0);
            expect(meta).toBeDefined();
            assertMeta(meta, '0x99aa06d3014798d86001c324468d497f');
        });

        it('command', async () => {
            const { frames, meta } = await wsClient.commandWithMeta(';', {}, []);
            expect(frames).toHaveLength(0);
            expect(meta).toBeDefined();
            assertMeta(meta, '0x99aa06d3014798d86001c324468d497f');
        });

        it('query', async () => {
            const { frames, meta } = await wsClient.queryWithMeta(';', {}, []);
            expect(frames).toHaveLength(0);
            expect(meta).toBeDefined();
            assertMeta(meta, '0x99aa06d3014798d86001c324468d497f');
        });

        it('send', async () => {
            const { result, meta } = await wsClient.sendWithMeta({
                id: 'test-send',
                type: 'Query',
                payload: {
                    rql: ';'
                }
            } as any);
            expect(result).toBeDefined();
            expect(meta).toBeDefined();
            assertMeta(meta, '0x99aa06d3014798d86001c324468d497f');
        });
    });

});

