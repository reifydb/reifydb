// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useSubscriptionExecutor, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database} from '../setup';
import {
    create_test_table_for_hook,
} from './subscription-test-helpers';

describe('useSubscriptionExecutor Hook', () => {
    beforeAll(async () => {
        await wait_for_database();
        const conn = get_connection({url: process.env.REIFYDB_WS_URL, token: process.env.REIFYDB_TOKEN});
        await conn.connect();
    }, 30000);

    afterAll(async () => {
        await clear_connection();
    });

    describe('Initial State', () => {
        it('should have correct initial state', () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            expect(result.current.state.data).toEqual([]);
            expect(result.current.state.changes).toEqual([]);
            expect(result.current.state.is_subscribed).toBe(false);
            expect(result.current.state.is_subscribing).toBe(false);
            expect(result.current.state.error).toBeUndefined();
            expect(result.current.state.subscription_id).toBeUndefined();
        });
    });

    describe('Subscription Lifecycle', () => {
        it('should successfully subscribe to a query', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_basic',
                ['id Int4', 'name Utf8']
            );

            expect(result.current.state.is_subscribing).toBe(false);
            expect(result.current.state.is_subscribed).toBe(false);

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), name: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            expect(result.current.state.is_subscribing).toBe(false);
            expect(result.current.state.error).toBeUndefined();
        });

        it('should set subscription_id when subscription succeeds', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_id',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.subscription_id).toBeDefined();
            });

            expect(typeof result.current.state.subscription_id).toBe('string');
            expect(result.current.state.subscription_id!.length).toBeGreaterThan(0);
        });

        it('should unsubscribe and reset subscription_id', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_unsub',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            const subscription_id = result.current.state.subscription_id;
            expect(subscription_id).toBeDefined();

            await act(async () => {
                await result.current.unsubscribe();
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(false);
            });

            expect(result.current.state.subscription_id).toBeUndefined();
        });

        it('should clean up subscription on unmount', async () => {
            const {result, unmount} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_cleanup',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // Unmount should trigger cleanup
            unmount();

            // Give cleanup time to execute
            await new Promise(resolve => setTimeout(resolve, 100));
        });

        it('should preserve data/changes after unsubscribe', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_preserve',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), value: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // Trigger an INSERT
            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, value: 'test'}]`,
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
            const table_name = await create_test_table_for_hook(
                'sub_changes',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), name: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            const beforeInsert = Date.now();

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, name: 'alice'}]`,
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

        it('should clear changes when clear_changes() is called', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_clear_changes',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            act(() => {
                result.current.clear_changes();
            });

            expect(result.current.state.changes).toEqual([]);
        });

        it('should clear data when clear_data() is called', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_clear_data',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // Even though data doesn't accumulate, we can still clear it
            act(() => {
                result.current.clear_data();
            });

            expect(result.current.state.data).toEqual([]);
        });

        it('should NOT modify data array on INSERT (data accumulation removed)', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_no_accumulate',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), name: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            expect(result.current.state.data).toEqual([]);

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]`,
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
            const table_name = await create_test_table_for_hook(
                'sub_insert',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), value: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, value: 'hello'}]`,
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
            const table_name = await create_test_table_for_hook(
                'sub_update',
                ['id Int4', 'value Utf8']
            );

            // Pre-populate with data
            const client = get_connection().get_client();
            await client!.command(
                `INSERT test::${table_name} [{id: 1, value: 'original'}]`,
                null,
                []
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), value: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            await act(async () => {
                await client!.command(
                    `UPDATE test::${table_name} { value: 'updated' } FILTER id == 1`,
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
            const table_name = await create_test_table_for_hook(
                'sub_remove',
                ['id Int4', 'value Utf8']
            );

            // Subscribe first so that the INSERT is tracked as a change
            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), value: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // Insert data after subscription is active
            const client = get_connection().get_client();
            await act(async () => {
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, value: 'to_delete'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');

            // Now delete the row
            await act(async () => {
                await client!.command(
                    `DELETE test::${table_name} FILTER id == 1`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[1].operation).toBe('REMOVE');
            expect(result.current.state.changes[1].rows).toEqual([
                {id: 1, value: 'to_delete'}
            ]);
        });

        it('should track multiple operation types in sequence', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_multi_ops',
                ['id Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), name: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // INSERT
            const client = get_connection().get_client();
            await act(async () => {
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, name: 'alice'}]`,
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
                    `UPDATE test::${table_name} { name: 'alice_updated' } FILTER id == 1`,
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
                    `DELETE test::${table_name} FILTER id == 1`,
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
            const table_name = await create_test_table_for_hook(
                'sub_batch',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name}`,
                    null,
                    Shape.object({id: Shape.number(), value: Shape.string()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            // Insert multiple rows in one operation
            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, value: 'first'}, {id: 2, value: 'second'}, {id: 3, value: 'third'}]`,
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
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });

            expect(result.current.state.is_subscribing).toBe(false);
            expect(result.current.state.is_subscribed).toBe(false);
        });

        it('should handle invalid query syntax', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'INVALID QUERY SYNTAX',
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });

            expect(result.current.state.is_subscribing).toBe(false);
        });

        it('should handle non-existent table', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'from test::table_that_does_not_exist_xyz',
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.error).toBeDefined();
            });
        });

        it('should set is_subscribing=false on error', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());

            await act(async () => {
                await result.current.subscribe(
                    'from invalid::syntax.here',
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribing).toBe(false);
            });

            expect(result.current.state.error).toBeDefined();
        });
    });

    describe('Edge Cases', () => {
        it('should handle multiple hook instances independently', async () => {
            const {result: result1} = renderHook(() => useSubscriptionExecutor());
            const {result: result2} = renderHook(() => useSubscriptionExecutor());

            const table1 = await create_test_table_for_hook(
                'sub_multi1',
                ['id Int4']
            );
            const table2 = await create_test_table_for_hook(
                'sub_multi2',
                ['id Int4']
            );

            await act(async () => {
                await result1.current.subscribe(
                    `from test::${table1}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
                await result2.current.subscribe(
                    `from test::${table2}`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result1.current.state.is_subscribed).toBe(true);
                expect(result2.current.state.is_subscribed).toBe(true);
            });

            // Trigger different operations on each
            await act(async () => {
                const client = get_connection().get_client();
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
            const table_name = await create_test_table_for_hook(
                'sub_empty',
                ['id Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${table_name} filter id > 1000`,
                    null,
                    Shape.object({id: Shape.number()})
                );
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            expect(result.current.state.data).toEqual([]);
            expect(result.current.state.changes).toEqual([]);
        });

        it('should handle subscription without shape', async () => {
            const {result} = renderHook(() => useSubscriptionExecutor());
            const table_name = await create_test_table_for_hook(
                'sub_no_shape',
                ['id Int4', 'value Utf8']
            );

            await act(async () => {
                await result.current.subscribe(`from test::${table_name}`);
            });

            await waitFor(() => {
                expect(result.current.state.is_subscribed).toBe(true);
            });

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} [{id: 1, value: 'test'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // Without shape, data comes through as raw Arrow values
            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows.length).toBe(1);
        });
    });
});
