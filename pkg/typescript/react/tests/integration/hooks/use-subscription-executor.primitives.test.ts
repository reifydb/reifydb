// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useSubscriptionExecutor, getConnection, clearConnection, Schema} from '../../../src';
import {waitForDatabase} from '../setup';
import {
    createTestTableForHook
} from './subscription-test-helpers';

describe('useSubscriptionExecutor - Primitive Schema Transformations', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection();
        await conn.connect();
    }, 30000);

    afterAll(async () => {
        await clearConnection();
    });

    describe('Number Types', () => {
        it('should transform Int4 to number', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_int4',
                ['id Int4', 'value Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, value: 42}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.id).toBe('number');
            expect(typeof row.value).toBe('number');
            expect(row.id).toBe(1);
            expect(row.value).toBe(42);
        });

        it('should transform Int8 to number', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_int8',
                ['id Int4', 'bigValue Int8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), bigValue: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, bigValue: 9007199254740991}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.bigValue).toBe('bigint');
            // @ts-ignore
            expect(row.bigValue).toBe(9007199254740991n);
        });

        it('should transform Float4 to number', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_float4',
                ['id Int4', 'floatValue Float4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), floatValue: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, floatValue: 3.14}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.floatValue).toBe('number');
            expect(row.floatValue).toBeCloseTo(3.14, 2);
        });

        it('should transform Float8 to number', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_float8',
                ['id Int4', 'doubleValue Float8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), doubleValue: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, doubleValue: 2.718281828459045}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.doubleValue).toBe('number');
            expect(row.doubleValue).toBeCloseTo(2.718281828459045, 10);
        });
    });

    describe('String Types', () => {
        it('should transform Utf8 to string', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_utf8',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, name: 'Alice'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.name).toBe('string');
            expect(row.name).toBe('Alice');
        });

        it('should handle unicode strings', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_unicode',
                ['id Int4', 'text Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), text: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, text: 'Hello 世界 🌍'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.text).toBe('Hello 世界 🌍');
        });
    });

    describe('Boolean Types', () => {
        it('should transform Boolean to boolean', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_bool',
                ['id Int4', 'isActive Boolean']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), isActive: Schema.boolean()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, isActive: true}, {id: 2, isActive: false}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const rows = result.current.state.changes[0].rows;
            expect(typeof rows[0].isActive).toBe('boolean');
            expect(typeof rows[1].isActive).toBe('boolean');
            expect(rows[0].isActive).toBe(true);
            expect(rows[1].isActive).toBe(false);
        });
    });

    describe('Mixed Primitive Objects', () => {
        it('should handle mixed primitive types', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_mixed',
                ['id Int4', 'name Utf8', 'score Float8', 'isValid Boolean']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),
                        name: Schema.string(),
                        score: Schema.number(),
                        isValid: Schema.boolean()
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, name: 'Alice', score: 95.5, isValid: true}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.id).toBe('number');
            expect(typeof row.name).toBe('string');
            expect(typeof row.score).toBe('number');
            expect(typeof row.isValid).toBe('boolean');
            expect(row).toEqual({
                id: 1,
                name: 'Alice',
                score: 95.5,
                isValid: true
            });
        });
    });

    describe('Operations with Transformations', () => {
        it('should apply transformations to INSERT operations', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'trans_insert',
                ['id Int4', 'name Utf8', 'value Float8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),
                        name: Schema.string(),
                        value: Schema.number()
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, name: 'test', value: 1.5}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows[0]).toEqual({
                id: 1,
                name: 'test',
                value: 1.5
            });
        });

        it('should apply transformations to UPDATE operations', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'trans_update',
                ['id Int4', 'value Int4']
            );

            // Pre-populate
            const client = getConnection().getClient();
            await client!.command(
                `INSERT test.${tableName} [{id: 1, value: 10}]`,
                null,
                []
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                await client!.command(
                    `UPDATE test.${tableName} { value: 20 } FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('UPDATE');
            expect(result.current.state.changes[1].rows[0]).toEqual({
                id: 1,
                value: 20
            });
        });

        it('should apply transformations to REMOVE operations', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'trans_remove',
                ['id Int4', 'name Utf8']
            );

            // Pre-populate
            const client = getConnection().getClient();
            await client!.command(
                `INSERT test.${tableName} [{id: 1, name: 'to_remove'}]`,
                null,
                []
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                await client!.command(
                    `DELETE test.${tableName} FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('REMOVE');
            expect(result.current.state.changes[1].rows[0]).toEqual({
                id: 1,
                name: 'to_remove'
            });
        });

        it('should maintain type consistency across multiple operations', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'trans_consistency',
                ['id Int4', 'count Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), count: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            const client = getConnection().getClient();

            // INSERT
            await act(async () => {
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, count: 0}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // UPDATE
            await act(async () => {
                await client!.command(
                    `UPDATE test.${tableName} { count: 5 } FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            // REMOVE
            await act(async () => {
                await client!.command(
                    `DELETE test.${tableName} FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(3);
            });

            // All operations should have consistent types
            result.current.state.changes.forEach(change => {
                change.rows.forEach(row => {
                    expect(typeof row.id).toBe('number');
                    expect(typeof row.count).toBe('number');
                });
            });
        });
    });

    describe('Edge Cases', () => {
        it('should handle undefined values with primitives', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_null',
                ['id Int4', 'optionalValue Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),
                        optionalValue: Schema.number()
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test.${tableName} [{id: 1, optionalValue: 42}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].rows[0].id).toBe(1);
        });

        it('should handle large batches with transformations', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'prim_batch',
                ['id Int4', 'value Float8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test.${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Insert 100 rows at once
            await act(async () => {
                const client = getConnection().getClient();
                const rows = Array.from({length: 100}, (_, i) => `{id: ${i}, value: ${i * 1.5}}`).join(', ');
                await client!.command(
                    `INSERT test.${tableName} [${rows}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].rows).toHaveLength(100);
            result.current.state.changes[0].rows.forEach((row, idx) => {
                expect(typeof row.id).toBe('number');
                expect(typeof row.value).toBe('number');
                expect(row.id).toBe(idx);
                expect(row.value).toBeCloseTo(idx * 1.5, 2);
            });
        });
    });
});
