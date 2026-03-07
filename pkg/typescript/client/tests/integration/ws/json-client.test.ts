// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, JsonWsClient} from "../../../src";


describe('JsonWsClient', () => {
    let client: JsonWsClient;

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        client = await Client.connect_json_ws(process.env.REIFYDB_WS_URL, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    }, 15000);

    afterEach(async () => {
        if (client) {
            try {
                client.disconnect();
            } catch (error) {
                // ignore
            }
            client = null;
        }
    });

    describe('query', () => {
        it('single statement returns typed rows', async () => {
            interface Result { result: number }
            const frames = await client.query<Result>('MAP {result: 42}');
            expect(frames).toHaveLength(1);
            expect(frames[0]).toHaveLength(1);
            expect(frames[0][0].result).toBe(42);
        }, 5000);

        it('string values', async () => {
            interface Result { name: string }
            const frames = await client.query<Result>("MAP {name: 'hello'}");
            expect(frames[0][0].name).toBe('hello');
        }, 5000);

        it('boolean values', async () => {
            interface Result { flag: boolean }
            const frames = await client.query<Result>("MAP {flag: true}");
            expect(frames[0][0].flag).toBe(true);
        }, 5000);

        it('null values', async () => {
            interface Result { val: null }
            const frames = await client.query<Result>("MAP {val: none}");
            expect(frames[0][0].val).toBeNull();
        }, 5000);

        it('float values', async () => {
            interface Result { pi: number }
            const frames = await client.query<Result>("MAP {pi: 3.14}");
            expect(frames[0][0].pi).toBeCloseTo(3.14);
        }, 5000);

        it('multiple rows', async () => {
            interface Result { n: number }
            const frames = await client.query<Result>(
                "OUTPUT MAP {n: 1}; OUTPUT MAP {n: 2}; MAP {n: 3}"
            );
            expect(frames).toHaveLength(3);
            expect(frames[0][0].n).toBe(1);
            expect(frames[1][0].n).toBe(2);
            expect(frames[2][0].n).toBe(3);
        }, 5000);

        it('multi-statement as array', async () => {
            interface R1 { a: number }
            interface R2 { b: string }
            const frames = await client.query(
                ["MAP {a: 1}", "MAP {b: 'two'}"]
            );
            expect(frames).toHaveLength(2);
            expect((frames[0][0] as any).a).toBe(1);
            expect((frames[1][0] as any).b).toBe('two');
        }, 5000);

        it('multiple columns', async () => {
            interface Result { x: number; y: string; z: boolean }
            const frames = await client.query<Result>("MAP {x: 10, y: 'abc', z: false}");
            expect(frames[0][0].x).toBe(10);
            expect(frames[0][0].y).toBe('abc');
            expect(frames[0][0].z).toBe(false);
        }, 5000);
    });

    describe('command', () => {
        it('single statement', async () => {
            interface Result { val: number }
            const frames = await client.command<Result>('MAP {val: 99}');
            expect(frames).toHaveLength(1);
            expect(frames[0][0].val).toBe(99);
        }, 5000);
    });

    describe('admin', () => {
        it('single statement', async () => {
            interface Result { val: number }
            const frames = await client.admin<Result>('MAP {val: 77}');
            expect(frames).toHaveLength(1);
            expect(frames[0][0].val).toBe(77);
        }, 5000);
    });

    describe('empty results', () => {
        it('empty statement', async () => {
            const frames = await client.query('');
            expect(frames).toHaveLength(0);
        }, 5000);

        it('semicolons only', async () => {
            const frames = await client.query(';;;');
            expect(frames).toHaveLength(0);
        }, 5000);
    });
});
