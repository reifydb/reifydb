// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Schema} from "@reifydb/core";

describe('Named Parameters - Primitive Types', () => {
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

    describe('command_named', () => {
        it('bool_param', async () => {
            const frames = await wsClient.command(
                "MAP $bool_val as result",
                {bool_val: true},
                [Schema.object({result: Schema.boolean()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: true}]);
        });

        it('int1_param', async () => {
            const frames = await wsClient.command(
                "MAP $int1_val as result",
                {int1_val: 42},
                [Schema.object({result: Schema.int1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 42}]);
        });

        it('int2_param', async () => {
            const frames = await wsClient.command(
                "MAP $int2_val as result",
                {int2_val: 1234},
                [Schema.object({result: Schema.int2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 1234}]);
        });

        it('int4_param', async () => {
            const frames = await wsClient.command(
                "MAP $int4_val as result",
                {int4_val: 12345678},
                [Schema.object({result: Schema.int4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 12345678}]);
        });

        it('int8_param', async () => {
            const frames = await wsClient.command(
                "MAP $int8_val as result",
                {int8_val: BigInt("9223372036854775807")},
                [Schema.object({result: Schema.int8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("9223372036854775807")}]);
        });

        it('int16_param', async () => {
            const frames = await wsClient.command(
                "MAP $int16_val as result",
                {int16_val: BigInt("170141183460469231731687303715884105727")},
                [Schema.object({result: Schema.int16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("170141183460469231731687303715884105727")}]);
        });

        it('uint1_param', async () => {
            const frames = await wsClient.command(
                "MAP $uint1_val as result",
                {uint1_val: 255},
                [Schema.object({result: Schema.uint1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 255}]);
        });

        it('uint2_param', async () => {
            const frames = await wsClient.command(
                "MAP $uint2_val as result",
                {uint2_val: 65535},
                [Schema.object({result: Schema.uint2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 65535}]);
        });

        it('uint4_param', async () => {
            const frames = await wsClient.command(
                "MAP $uint4_val as result",
                {uint4_val: 4294967295},
                [Schema.object({result: Schema.uint4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 4294967295n}]);
        });

        it('uint8_param', async () => {
            const frames = await wsClient.command(
                "MAP $uint8_val as result",
                {uint8_val: BigInt("18446744073709551615")},
                [Schema.object({result: Schema.uint8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("18446744073709551615")}]);
        });

        it('uint16_param', async () => {
            const frames = await wsClient.command(
                "MAP $uint16_val as result",
                {uint16_val: BigInt("340282366920938463463374607431768211455")},
                [Schema.object({result: Schema.uint16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("340282366920938463463374607431768211455")}]);
        });

        it('float4_param', async () => {
            const frames = await wsClient.command(
                "MAP $float4_val as result",
                {float4_val: 3.14},
                [Schema.object({result: Schema.float4()})]
            );
            expect(frames).toHaveLength(1);
            // Use approximate comparison for floats
            expect(frames[0][0].result).toBeCloseTo(3.14, 2);
        });

        it('float8_param', async () => {
            const frames = await wsClient.command(
                "MAP $float8_val as result",
                {float8_val: 3.141592653589793},
                [Schema.object({result: Schema.float8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 10);
        });

        it('decimal_param', async () => {
            const frames = await wsClient.command(
                "MAP $decimal_val as result",
                {decimal_val: "123.456789"},
                [Schema.object({result: Schema.decimal()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: "123.456789"}]);
        });
    });

    describe('query_named', () => {
        it('bool_param', async () => {
            const frames = await wsClient.query(
                "MAP $bool_val as result",
                {bool_val: true},
                [Schema.object({result: Schema.boolean()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: true}]);
        });

        it('int1_param', async () => {
            const frames = await wsClient.query(
                "MAP $int1_val as result",
                {int1_val: 42},
                [Schema.object({result: Schema.int1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 42}]);
        });

        it('int2_param', async () => {
            const frames = await wsClient.query(
                "MAP $int2_val as result",
                {int2_val: 1234},
                [Schema.object({result: Schema.int2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 1234}]);
        });

        it('int4_param', async () => {
            const frames = await wsClient.query(
                "MAP $int4_val as result",
                {int4_val: 12345678},
                [Schema.object({result: Schema.int4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 12345678}]);
        });

        it('int8_param', async () => {
            const frames = await wsClient.query(
                "MAP $int8_val as result",
                {int8_val: BigInt("9223372036854775807")},
                [Schema.object({result: Schema.int8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("9223372036854775807")}]);
        });

        it('int16_param', async () => {
            const frames = await wsClient.query(
                "MAP $int16_val as result",
                {int16_val: BigInt("170141183460469231731687303715884105727")},
                [Schema.object({result: Schema.int16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("170141183460469231731687303715884105727")}]);
        });

        it('uint1_param', async () => {
            const frames = await wsClient.query(
                "MAP $uint1_val as result",
                {uint1_val: 255},
                [Schema.object({result: Schema.uint1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 255}]);
        });

        it('uint2_param', async () => {
            const frames = await wsClient.query(
                "MAP $uint2_val as result",
                {uint2_val: 65535},
                [Schema.object({result: Schema.uint2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 65535}]);
        });

        it('uint4_param', async () => {
            const frames = await wsClient.query(
                "MAP $uint4_val as result",
                {uint4_val: 4294967295},
                [Schema.object({result: Schema.uint4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 4294967295n}]);
        });

        it('uint8_param', async () => {
            const frames = await wsClient.query(
                "MAP $uint8_val as result",
                {uint8_val: BigInt("18446744073709551615")},
                [Schema.object({result: Schema.uint8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("18446744073709551615")}]);
        });

        it('uint16_param', async () => {
            const frames = await wsClient.query(
                "MAP $uint16_val as result",
                {uint16_val: BigInt("340282366920938463463374607431768211455")},
                [Schema.object({result: Schema.uint16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("340282366920938463463374607431768211455")}]);
        });

        it('float4_param', async () => {
            const frames = await wsClient.query(
                "MAP $float4_val as result",
                {float4_val: 3.14},
                [Schema.object({result: Schema.float4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.14, 2);
        });

        it('float8_param', async () => {
            const frames = await wsClient.query(
                "MAP $float8_val as result",
                {float8_val: 3.141592653589793},
                [Schema.object({result: Schema.float8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 10);
        });

        it('decimal_param', async () => {
            const frames = await wsClient.query(
                "MAP $decimal_val as result",
                {decimal_val: "123.456789"},
                [Schema.object({result: Schema.decimal()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: "123.456789"}]);
        });
    });
});