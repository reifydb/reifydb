/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, beforeAll, beforeEach, describe, expect, it} from 'vitest';
import {waitForDatabase} from "../setup";
import {
    Client, 
    WsClient, 
    Schema, 
    BidirectionalSchema
} from "../../../src";

describe('Schema Integration - Simple', () => {
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
        it('should handle simple primitive mapping', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    value: Schema.string()
                }),
                result: Schema.object({
                    result: Schema.string()
                })
            };

            const result = await wsClient.command<{
                result: string;
            }>(
                'MAP $value as result',
                { value: "hello schema" },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            expect(typeof row.result).toBe('string');
            expect(row.result).toBe('hello schema');
        }, 2000);

        it('should handle number parameters', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    num: Schema.number()
                }),
                result: Schema.object({
                    result: Schema.number()
                })
            };

            const result = await wsClient.command<{
                result: number;
            }>(
                'MAP $num as result',
                { num: 42 },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            expect(typeof row.result).toBe('number');
            expect(row.result).toBe(42);
        }, 2000);

        it('should handle boolean parameters', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    flag: Schema.boolean()
                }),
                result: Schema.object({
                    result: Schema.boolean()
                })
            };

            const result = await wsClient.command<{
                result: boolean;
            }>(
                'MAP $flag as result',
                { flag: true },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            expect(typeof row.result).toBe('boolean');
            expect(row.result).toBe(true);
        }, 2000);

        it('should handle array parameters', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.array(Schema.string()),
                result: Schema.object({
                    first: Schema.string(),
                    second: Schema.string()
                })
            };

            const result = await wsClient.command<{
                first: string;
                second: string;
            }>(
                'MAP { $1 as first, $2 as second }',
                ["apple", "banana"],
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            expect(row.first).toBe("apple");
            expect(row.second).toBe("banana");
        }, 2000);
    });

    describe('query', () => {
        it('should handle simple primitive mapping', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    value: Schema.number()
                }),
                result: Schema.object({
                    result: Schema.number()
                })
            };

            const result = await wsClient.query<{
                result: number;
            }>(
                'MAP $value as result',
                { value: 123 },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);
            
            const row = result[0][0];
            expect(typeof row.result).toBe('number');
            expect(row.result).toBe(123);
        }, 2000);

        it('should demonstrate schema vs non-schema comparison', async () => {
            // Demonstrate schema decoding - primitive results
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    value: Schema.string()
                }),
                result: Schema.object({
                    result: Schema.string()
                })
            };

            const resultWithSchema = await wsClient.query<{
                result: string;
            }>(
                'MAP $value as result',
                { value: "test" },
                schema
            );

            // With schema returns primitive
            expect(typeof resultWithSchema[0][0].result).toBe('string');
            expect(resultWithSchema[0][0].result).toBe('test');
            
            // Demonstrate without result schema (returns raw Value objects)
            const schemaNoResult: BidirectionalSchema = {
                params: Schema.object({
                    value: Schema.string()
                })
                // No result schema - returns Value objects
            };
            
            const resultNoSchema = await wsClient.query(
                'MAP $value as result',
                { value: "test" },
                schemaNoResult
            );
            
            // Without result schema returns Value object
            expect(typeof resultNoSchema[0][0].result).toBe('object');
            expect(resultNoSchema[0][0].result.value).toBe('test');
        }, 2000);
    });
});