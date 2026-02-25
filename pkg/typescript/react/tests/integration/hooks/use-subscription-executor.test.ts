// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useSubscriptionExecutor, getConnection, clearConnection, Schema} from '../../../src';
import {waitForDatabase} from '../setup';
import {
    createTestTableForHook,
} from './subscription-test-helpers';

describe('useSubscriptionExecutor Hook', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection();
        await conn.connect();
    }, 30000);

    afterAll(async () => {
        await clearConnection();
    });

    describe('Initial State', () => {
        it('should have correct initial state', () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            expect(result.current.state.data).toEqual([]);
            expect(result.current.state.changes).toEqual([]);
            expect(result.current.state.isSubscribed).toBe(false);
            expect(result.current.state.isSubscribing).toBe(false);
            expect(result.current.state.error).toBeUndefined();
            expect(result.current.state.subscriptionId).toBeUndefined();
        });
    });

    describe('Subscription Lifecycle', () => {
        it('should successfully subscribe to a query', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_basic',
                ['id Int4', 'name Utf8']
            );

            expect(result.current.state.isSubscribing).toBe(false);
            expect(result.current.state.isSubscribed).toBe(false);

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            expect(result.current.state.isSubscribing).toBe(false);
            expect(result.current.state.error).toBeUndefined();
        });

        it('should set subscriptionId when subscription succeeds', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_id',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.subscriptionId).toBeDefined();
            });

            expect(typeof result.current.state.subscriptionId).toBe('string');
            expect(result.current.state.subscriptionId!.length).toBeGreaterThan(0);
        });

        it('should unsubscribe and reset subscriptionId', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_unsub',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            const subscriptionId = result.current.state.subscriptionId;
            expect(subscriptionId).toBeDefined();

            await act(async () => {
                await result.current.unsubscribe();
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(false);
            });

            expect(result.current.state.subscriptionId).toBeUndefined();
        });

        it('should clean up subscription on unmount', async () => {
            const {result, unmount} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_cleanup',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Unmount should trigger cleanup
            unmount();

            // Give cleanup time to execute
            await new Promise(resolve => setTimeout(resolve, 100));
        });

        it('should preserve data/changes after unsubscribe', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_preserve',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Trigger an INSERT
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, value: 'test'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const changesBeforeUnsub = result.current.state.changes;
            const dataBeforeUnsub = result.current.state.data;

            await act(async () => {
                await result.current.unsubscribe();
            });

            expect(result.current.state.changes).toEqual(changesBeforeUnsub);
            expect(result.current.state.data).toEqual(dataBeforeUnsub);
        });
    });

    describe('State Management', () => {
        it('should accumulate change events with timestamps', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_changes',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            const beforeInsert = Date.now();

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, name: 'alice'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const change = result.current.state.changes[0];
            expect(change.operation).toBe('INSERT');
            expect(change.rows).toEqual([{id: 1, name: 'alice'}]);
            expect(change.timestamp).toBeGreaterThanOrEqual(beforeInsert);
            expect(change.timestamp).toBeLessThanOrEqual(Date.now());
        });

        it('should clear changes when clearChanges() is called', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_clear_changes',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            act(() => {
                result.current.clearChanges();
            });

            expect(result.current.state.changes).toEqual([]);
        });

        it('should clear data when clearData() is called', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_clear_data',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Even though data doesn't accumulate, we can still clear it
            act(() => {
                result.current.clearData();
            });

            expect(result.current.state.data).toEqual([]);
        });

        it('should NOT modify data array on INSERT (data accumulation removed)', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_no_accumulate',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            expect(result.current.state.data).toEqual([]);

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // Data should remain empty (not accumulated)
            expect(result.current.state.data).toEqual([]);
            // But changes should be tracked
            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows).toEqual([
                {id: 1, name: 'alice'},
                {id: 2, name: 'bob'}
            ]);
        });
    });

    describe('Operation Callbacks', () => {
        it('should track INSERT operations in changes array', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_insert',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, value: 'hello'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows).toEqual([
                {id: 1, value: 'hello'}
            ]);
        });

        it('should track UPDATE operations in changes array', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_update',
                ['id Int4', 'value Utf8']
            );

            // Pre-populate with data
            const client = getConnection().getClient();
            await client!.command(
                `INSERT test::${tableName} [{id: 1, value: 'original'}]`,
                null,
                []
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                await client!.command(
                    `UPDATE test::${tableName} { value: 'updated' } FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('UPDATE');
            expect(result.current.state.changes[1].rows).toEqual([
                {id: 1, value: 'updated'}
            ]);
        });

        it('should track REMOVE operations in changes array', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_remove',
                ['id Int4', 'value Utf8']
            );

            // Pre-populate with data
            const client = getConnection().getClient();
            await client!.command(
                `INSERT test::${tableName} [{id: 1, value: 'to_delete'}]`,
                null,
                []
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                await client!.command(
                    `DELETE test::${tableName} FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('REMOVE');
            expect(result.current.state.changes[1].rows).toEqual([
                {id: 1, value: 'to_delete'}
            ]);
        });

        it('should track multiple operation types in sequence', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_multi_ops',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), name: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // INSERT
            const client = getConnection().getClient();
            await act(async () => {
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, name: 'alice'}]`,
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
                    `UPDATE test::${tableName} { name: 'alice_updated' } FILTER id == 1`,
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
                    `DELETE test::${tableName} FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(3);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('UPDATE');
            expect(result.current.state.changes[2].operation).toBe('REMOVE');
        });

        it('should batch multiple rows in single operation', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_batch',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({id: Schema.number(), value: Schema.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Insert multiple rows in one operation
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, value: 'first'}, {id: 2, value: 'second'}, {id: 3, value: 'third'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows).toHaveLength(3);
            expect(result.current.state.changes[0].rows).toEqual([
                {id: 1, value: 'first'},
                {id: 2, value: 'second'},
                {id: 3, value: 'third'}
            ]);
        });
    });

    describe('Error Handling', () => {
        it('should set error when subscription fails', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'from nonexistent::table',
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });

            expect(result.current.state.isSubscribing).toBe(false);
            expect(result.current.state.isSubscribed).toBe(false);
        });

        it('should handle invalid query syntax', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'INVALID QUERY SYNTAX',
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });

            expect(result.current.state.isSubscribing).toBe(false);
        });

        it('should handle non-existent table', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'from test::table_that_does_not_exist_xyz',
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });
        });

        it('should set isSubscribing=false on error', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'from invalid::syntax.here',
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribing).toBe(false);
            });

            expect(result.current.state.error).toBeDefined();
        });
    });

    describe('Edge Cases', () => {
        it('should handle multiple hook instances independently', async () => {
            const {result: result1} = renderHook(() => useSubscriptionExecutor());
            const {result: result2} = renderHook(() => useSubscriptionExecutor());

            const table1 = await createTestTableForHook(
                'sub_multi1',
                ['id Int4']
            );
            const table2 = await createTestTableForHook(
                'sub_multi2',
                ['id Int4']
            );

            await act(async () => {
                await result1.current.subscribe(
                    `from test::${table1}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
                await result2.current.subscribe(
                    `from test::${table2}`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result1.current.state.isSubscribed).toBe(true);
                expect(result2.current.state.isSubscribed).toBe(true);
            });

            // Trigger different operations on each
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${table1} [{id: 100}]`, null, []);
                await client!.command(`INSERT test::${table2} [{id: 200}]`, null, []);
            });

            await waitFor(() => {
                expect(result1.current.state.changes.length).toBe(1);
                expect(result2.current.state.changes.length).toBe(1);
            });

            expect(result1.current.state.changes[0].rows[0].id).toBe(100);
            expect(result2.current.state.changes[0].rows[0].id).toBe(200);
        });

        it('should handle empty result sets', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_empty',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName} filter id > 1000`,
                    null,
                    Schema.object({id: Schema.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            expect(result.current.state.data).toEqual([]);
            expect(result.current.state.changes).toEqual([]);
        });

        it('should handle subscription without schema', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'sub_no_schema',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(`from test::${tableName}`);
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} [{id: 1, value: 'test'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // Without schema, data comes through as raw Arrow values
            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows.length).toBe(1);
        });
    });
});
