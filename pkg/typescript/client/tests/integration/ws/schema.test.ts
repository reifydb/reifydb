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

describe('Schema Integration', () => {
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
        it('should handle primitive parameters and results', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    name: Schema.string(),
                    age: Schema.number(),
                    active: Schema.boolean()
                }),
                result: Schema.object({
                    greeting: Schema.string(),
                    info: Schema.string(),
                    status: Schema.boolean()
                })
            };

            const result = await wsClient.command<{
                greeting: string;
                info: string;
                status: boolean;
            }>(
                'MAP { CONCAT("Hello ", $name) as greeting, CONCAT("Age: ", CAST($age as TEXT)) as info, $active as status }',
                {
                    name: "Alice",
                    age: 25,
                    active: true
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(typeof row.greeting).toBe('string');
            expect(row.greeting).toBe('Hello Alice');
            expect(typeof row.info).toBe('string');
            expect(row.info).toBe('Age: 25');
            expect(typeof row.status).toBe('boolean');
            expect(row.status).toBe(true);
        }, 1000);

        it('should handle mixed primitive and auto-detection', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    id: Schema.number(),
                    data: Schema.auto() // Auto-detect type
                }),
                result: Schema.object({
                    id_field: Schema.number(),
                    data_field: Schema.string()
                })
            };

            const result = await wsClient.command<{
                id_field: number;
                data_field: string;
            }>(
                'MAP { $id as id_field, CAST($data as TEXT) as data_field }',
                {
                    id: 42,
                    data: "test data"
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(typeof row.id_field).toBe('number');
            expect(row.id_field).toBe(42);
            expect(typeof row.data_field).toBe('string');
            expect(row.data_field).toBe('test data');
        }, 1000);

        it('should handle array parameters', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.array(Schema.string()),
                result: Schema.object({
                    first: Schema.string(),
                    second: Schema.string(),
                    third: Schema.string()
                })
            };

            const result = await wsClient.command<{
                first: string;
                second: string;
                third: string;
            }>(
                'MAP { $1 as first, $2 as second, $3 as third }',
                ["apple", "banana", "cherry"],
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(row.first).toBe("apple");
            expect(row.second).toBe("banana");
            expect(row.third).toBe("cherry");
        }, 1000);

        it('should handle optional fields', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    required: Schema.string(),
                    optional: Schema.optional(Schema.string())
                }),
                result: Schema.object({
                    required_field: Schema.string(),
                    optional_field: Schema.optional(Schema.string())
                })
            };

            const result = await wsClient.command<{
                required_field: string;
                optional_field?: string;
            }>(
                'MAP { $required as required_field, $optional as optional_field }',
                {
                    required: "must have",
                    optional: undefined
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(row.required_field).toBe("must have");
            expect(row.optional_field).toBe(undefined);
        }, 1000);
    });

    describe('query', () => {
        it('should handle primitive parameters and results', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    multiplier: Schema.number(),
                    base: Schema.number()
                }),
                result: Schema.object({
                    calculation: Schema.number(),
                    description: Schema.string()
                })
            };

            const result = await wsClient.query<{
                calculation: number;
                description: string;
            }>(
                'MAP { ($base * $multiplier) as calculation, CONCAT("Result: ", CAST(($base * $multiplier) as TEXT)) as description }',
                {
                    multiplier: 3,
                    base: 7
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(typeof row.calculation).toBe('number');
            expect(row.calculation).toBe(21);
            expect(typeof row.description).toBe('string');
            expect(row.description).toBe('Result: 21');
        }, 1000);

        it('should handle complex nested schema', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    user: Schema.object({
                        name: Schema.string(),
                        age: Schema.number()
                    }),
                    settings: Schema.object({
                        theme: Schema.string(),
                        notifications: Schema.boolean()
                    })
                }),
                result: Schema.object({
                    user_info: Schema.string(),
                    preferences: Schema.string()
                })
            };

            const result = await wsClient.query<{
                user_info: string;
                preferences: string;
            }>(
                'MAP { CONCAT($user.name, " (", CAST($user.age as TEXT), ")") as user_info, CONCAT("Theme: ", $settings.theme, ", Notifications: ", CAST($settings.notifications as TEXT)) as preferences }',
                {
                    user: {
                        name: "Bob",
                        age: 30
                    },
                    settings: {
                        theme: "dark",
                        notifications: true
                    }
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(row.user_info).toBe("Bob (30)");
            expect(row.preferences).toBe("Theme: dark, Notifications: true");
        }, 1000);

        it('should handle date and bigint types', async () => {
            const schema: BidirectionalSchema = {
                params: Schema.object({
                    timestamp: Schema.date(),
                    count: Schema.bigint()
                }),
                result: Schema.object({
                    date_str: Schema.string(),
                    count_str: Schema.string()
                })
            };

            const testDate = new Date('2024-01-15T10:30:00Z');
            const testBigInt = BigInt('9007199254740991');

            const result = await wsClient.query<{
                date_str: string;
                count_str: string;
            }>(
                'MAP { CAST($timestamp as TEXT) as date_str, CAST($count as TEXT) as count_str }',
                {
                    timestamp: testDate,
                    count: testBigInt
                },
                schema
            );

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveLength(1);

            const row = result[0][0];
            expect(typeof row.date_str).toBe('string');
            expect(row.date_str).toBe(testDate.toISOString());
            expect(typeof row.count_str).toBe('string');
            expect(row.count_str).toBe(testBigInt.toString());
        }, 1000);
    });
});