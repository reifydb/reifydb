// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, afterEach, describe, expect, it} from 'vitest';
import {renderHook, waitFor, act} from '@testing-library/react';
import {useSubscription, ConnectionProvider, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database} from '../setup';
import {create_test_table_for_hook} from './subscription-test-helpers';
// @ts-ignore
import React from 'react';

describe('useSubscription Hook', () => {
    const wrapper = ({children}: { children: React.ReactNode }) => (
        <ConnectionProvider config={{url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN}} children={children}/>
    );

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    afterEach(async () => {
        await clear_connection();
        // Give connections time to clean up
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clear_connection();
    });

    describe('Auto-subscribe Behavior', () => {
        it('should auto-subscribe on mount when enabled=true (default)', async () => {
            const table_name = await create_test_table_for_hook(
                'auto_sub',
                ['id Int4', 'name Utf8']
            );

            const shape = Shape.object({id: Shape.number(), name: Shape.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape
                ),
                {wrapper}
            );

            // Should start subscribing
            expect(result.current.is_subscribing || result.current.is_subscribed).toBe(true);

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            expect(result.current.is_subscribing).toBe(false);
            expect(result.current.error).toBeUndefined();
            expect(result.current.subscription_id).toBeDefined();
        });

        it('should not subscribe when enabled=false', async () => {
            const table_name = await create_test_table_for_hook(
                'no_auto_sub',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape,
                    {enabled: false}
                ),
                {wrapper}
            );

            // Give it a moment to potentially subscribe (it shouldn't)
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(result.current.is_subscribed).toBe(false);
            expect(result.current.is_subscribing).toBe(false);
            expect(result.current.subscription_id).toBeUndefined();
        });

        it('should subscribe when enabled switches from false to true', async () => {
            const table_name = await create_test_table_for_hook(
                'enable_toggle',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape,
                    {enabled}
                ),
                {initialProps: {enabled: false}, wrapper}
            );

            expect(result.current.is_subscribed).toBe(false);

            // Enable subscription
            rerender({enabled: true});

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            expect(result.current.subscription_id).toBeDefined();
        });

        it('should unsubscribe when enabled switches from true to false', async () => {
            const table_name = await create_test_table_for_hook(
                'disable_toggle',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape,
                    {enabled}
                ),
                {initialProps: {enabled: true}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            const subscription_id = result.current.subscription_id;
            expect(subscription_id).toBeDefined();

            // Disable subscription
            rerender({enabled: false});

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(false);
            });

            expect(result.current.subscription_id).toBeUndefined();
        });

        it('should unsubscribe on unmount', async () => {
            const table_name = await create_test_table_for_hook(
                'unmount_cleanup',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, unmount} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            // Unmount should trigger cleanup
            unmount();

            // Give cleanup time to execute
            await new Promise(resolve => setTimeout(resolve, 100));
        });
    });

    describe('Dependency Re-subscription', () => {
        it('should re-subscribe when query changes', async () => {
            const table1 = await create_test_table_for_hook(
                'query_change1',
                ['id Int4']
            );
            const table2 = await create_test_table_for_hook(
                'query_change2',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, rerender} = renderHook(
                ({query}) => useSubscription(
                    query,
                    null,
                    shape
                ),
                {initialProps: {query: `from test::${table1}`}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            const first_sub_id = result.current.subscription_id;

            // Change query
            rerender({query: `from test::${table2}`});

            await waitFor(() => {
                expect(result.current.subscription_id).not.toBe(first_sub_id);
            });

            expect(result.current.is_subscribed).toBe(true);
        });

        it('should re-subscribe when params change', async () => {
            const table_name = await create_test_table_for_hook(
                'params_change',
                ['id Int4', 'value Int4']
            );

            const shape = Shape.object({id: Shape.number(), value: Shape.number()});
            const {result, rerender} = renderHook(
                ({params}) => useSubscription(
                    `from test::${table_name} filter value == $val`,
                    params,
                    shape
                ),
                {initialProps: {params: {val: 1}}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            const first_sub_id = result.current.subscription_id;

            // Change params
            rerender({params: {val: 2}});

            await waitFor(() => {
                expect(result.current.subscription_id).not.toBe(first_sub_id);
            });

            expect(result.current.is_subscribed).toBe(true);
        });

        it('should re-subscribe when shape changes', async () => {
            const table_name = await create_test_table_for_hook(
                'shape_change',
                ['id Int4', 'name Utf8', 'value Int4']
            );

            const shape1 = Shape.object({id: Shape.number(), name: Shape.string()});
            const shape2 = Shape.object({id: Shape.number(), value: Shape.number()});

            const {result, rerender} = renderHook(
                ({shape}) => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape as any
                ),
                {initialProps: {shape: shape1}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            const first_sub_id = result.current.subscription_id;

            // Change shape
            rerender({shape: shape2});

            await waitFor(() => {
                expect(result.current.subscription_id).not.toBe(first_sub_id);
            });

            expect(result.current.is_subscribed).toBe(true);
        });

        it('should get new subscription_id after re-subscription', async () => {
            const table1 = await create_test_table_for_hook(
                'resub_id1',
                ['id Int4']
            );
            const table2 = await create_test_table_for_hook(
                'resub_id2',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, rerender} = renderHook(
                ({query}) => useSubscription(
                    query,
                    null,
                    shape
                ),
                {initialProps: {query: `from test::${table1}`}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            const first_sub_id = result.current.subscription_id;
            expect(first_sub_id).toBeDefined();

            // Change query to trigger re-subscription
            rerender({query: `from test::${table2}`});

            await waitFor(() => {
                const new_sub_id = result.current.subscription_id;
                expect(new_sub_id).toBeDefined();
                expect(new_sub_id).not.toBe(first_sub_id);
            });
        });
    });

    describe('ConnectionProvider Integration', () => {
        it('should work with ConnectionProvider wrapper', async () => {
            const table_name = await create_test_table_for_hook(
                'provider_test',
                ['id Int4', 'name Utf8']
            );

            const shape = Shape.object({id: Shape.number(), name: Shape.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            // Trigger an INSERT
            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} FROM [{id: 1, name: 'test'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.changes.length).toBe(1);
            });

            expect(result.current.changes[0].operation).toBe('INSERT');
            expect(result.current.changes[0].rows[0]).toEqual({id: 1, name: 'test'});
        });

        it('should work with connection config override', async () => {
            const table_name = await create_test_table_for_hook(
                'config_override',
                ['id Int4']
            );

            const override_config = {url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN, options: {timeout_ms: 2000}};
            const shape = Shape.object({id: Shape.number()});

            const {result, unmount} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape,
                    {connection_config: override_config}
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            expect(result.current.error).toBeUndefined();

            // Clean up
            unmount();
            await clear_connection();
        });

        it('should flatten state from executor correctly', async () => {
            const table_name = await create_test_table_for_hook(
                'flatten_state',
                ['id Int4', 'value Utf8']
            );

            const shape = Shape.object({id: Shape.number(), value: Shape.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            // Check that all state properties are accessible at top level
            expect(result.current.data).toBeDefined();
            expect(result.current.changes).toBeDefined();
            expect(result.current.is_subscribed).toBe(true);
            expect(result.current.is_subscribing).toBe(false);
            expect(result.current.error).toBeUndefined();
            expect(result.current.subscription_id).toBeDefined();
        });
    });

    describe('Operation Tracking', () => {
        it('should track INSERT operations', async () => {
            const table_name = await create_test_table_for_hook(
                'track_insert',
                ['id Int4', 'name Utf8']
            );

            const shape = Shape.object({id: Shape.number(), name: Shape.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            await act(async () => {
                const client = get_connection().get_client();
                await client!.command(
                    `INSERT test::${table_name} FROM [{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]`,
                    null,
                    []
                );
            });

            await waitFor(() => {
                expect(result.current.changes.length).toBe(1);
            });

            expect(result.current.changes[0].operation).toBe('INSERT');
            expect(result.current.changes[0].rows).toHaveLength(2);
        });
    });

    describe('Edge Cases', () => {
        it('should handle rapid subscribe/unsubscribe cycles', async () => {
            const table_name = await create_test_table_for_hook(
                'rapid_toggle',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${table_name}`,
                    null,
                    shape,
                    {enabled}
                ),
                {initialProps: {enabled: true}, wrapper}
            );

            // Rapid toggling
            rerender({enabled: false});
            rerender({enabled: true});
            rerender({enabled: false});
            rerender({enabled: true});

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            expect(result.current.error).toBeUndefined();
        });

        it('should handle empty result sets', async () => {
            const table_name = await create_test_table_for_hook(
                'empty_results',
                ['id Int4']
            );

            const shape = Shape.object({id: Shape.number()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${table_name} filter id > 1000`,
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_subscribed).toBe(true);
            });

            expect(result.current.data).toEqual([]);
            expect(result.current.changes).toEqual([]);
        });

        it('should handle subscription errors', async () => {
            const shape = Shape.object({id: Shape.number()});
            const {result} = renderHook(
                () => useSubscription(
                    'from nonexistent::table',
                    null,
                    shape
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.is_subscribed).toBe(false);
        });
    });
});
