// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {Client, WsClient} from "../../../src";
import {Shape} from "@reifydb/core";

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
                "MAP {result: $bool_val}",
                {bool_val: true},
                [Shape.object({result: Shape.boolean()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: true}]);
        });

        it('int1_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $int1_val}",
                {int1_val: 42},
                [Shape.object({result: Shape.int1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 42}]);
        });

        it('int2_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $int2_val}",
                {int2_val: 1234},
                [Shape.object({result: Shape.int2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 1234}]);
        });

        it('int4_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $int4_val}",
                {int4_val: 12345678},
                [Shape.object({result: Shape.int4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 12345678}]);
        });

        it('int8_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $int8_val}",
                {int8_val: BigInt("9223372036854775807")},
                [Shape.object({result: Shape.int8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("9223372036854775807")}]);
        });

        it('int16_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $int16_val}",
                {int16_val: BigInt("170141183460469231731687303715884105727")},
                [Shape.object({result: Shape.int16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("170141183460469231731687303715884105727")}]);
        });

        it('uint1_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $uint1_val}",
                {uint1_val: 255},
                [Shape.object({result: Shape.uint1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 255}]);
        });

        it('uint2_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $uint2_val}",
                {uint2_val: 65535},
                [Shape.object({result: Shape.uint2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 65535}]);
        });

        it('uint4_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $uint4_val}",
                {uint4_val: 4294967295},
                [Shape.object({result: Shape.uint4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 4294967295n}]);
        });

        it('uint8_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $uint8_val}",
                {uint8_val: BigInt("18446744073709551615")},
                [Shape.object({result: Shape.uint8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("18446744073709551615")}]);
        });

        it('uint16_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $uint16_val}",
                {uint16_val: BigInt("340282366920938463463374607431768211455")},
                [Shape.object({result: Shape.uint16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("340282366920938463463374607431768211455")}]);
        });

        it('float4_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $float4_val}",
                {float4_val: 3.14},
                [Shape.object({result: Shape.float4()})]
            );
            expect(frames).toHaveLength(1);
            // Use approximate comparison for floats
            expect(frames[0][0].result).toBeCloseTo(3.14, 2);
        });

        it('float8_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $float8_val}",
                {float8_val: 3.141592653589793},
                [Shape.object({result: Shape.float8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 10);
        });

        it('decimal_param', async () => {
            const frames = await wsClient.command(
                "MAP {result: $decimal_val}",
                {decimal_val: "123.456789"},
                [Shape.object({result: Shape.decimal()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: "123.456789"}]);
        });
    });

    describe('query_named', () => {
        it('bool_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $bool_val}",
                {bool_val: true},
                [Shape.object({result: Shape.boolean()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: true}]);
        });

        it('int1_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $int1_val}",
                {int1_val: 42},
                [Shape.object({result: Shape.int1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 42}]);
        });

        it('int2_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $int2_val}",
                {int2_val: 1234},
                [Shape.object({result: Shape.int2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 1234}]);
        });

        it('int4_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $int4_val}",
                {int4_val: 12345678},
                [Shape.object({result: Shape.int4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 12345678}]);
        });

        it('int8_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $int8_val}",
                {int8_val: BigInt("9223372036854775807")},
                [Shape.object({result: Shape.int8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("9223372036854775807")}]);
        });

        it('int16_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $int16_val}",
                {int16_val: BigInt("170141183460469231731687303715884105727")},
                [Shape.object({result: Shape.int16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("170141183460469231731687303715884105727")}]);
        });

        it('uint1_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $uint1_val}",
                {uint1_val: 255},
                [Shape.object({result: Shape.uint1()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 255}]);
        });

        it('uint2_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $uint2_val}",
                {uint2_val: 65535},
                [Shape.object({result: Shape.uint2()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 65535}]);
        });

        it('uint4_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $uint4_val}",
                {uint4_val: 4294967295},
                [Shape.object({result: Shape.uint4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: 4294967295n}]);
        });

        it('uint8_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $uint8_val}",
                {uint8_val: BigInt("18446744073709551615")},
                [Shape.object({result: Shape.uint8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("18446744073709551615")}]);
        });

        it('uint16_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $uint16_val}",
                {uint16_val: BigInt("340282366920938463463374607431768211455")},
                [Shape.object({result: Shape.uint16()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: BigInt("340282366920938463463374607431768211455")}]);
        });

        it('float4_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $float4_val}",
                {float4_val: 3.14},
                [Shape.object({result: Shape.float4()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.14, 2);
        });

        it('float8_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $float8_val}",
                {float8_val: 3.141592653589793},
                [Shape.object({result: Shape.float8()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0][0].result).toBeCloseTo(3.141592653589793, 10);
        });

        it('decimal_param', async () => {
            const frames = await wsClient.query(
                "MAP {result: $decimal_val}",
                {decimal_val: "123.456789"},
                [Shape.object({result: Shape.decimal()})]
            );
            expect(frames).toHaveLength(1);
            expect(frames[0]).toEqual([{result: "123.456789"}]);
        });
    });
});