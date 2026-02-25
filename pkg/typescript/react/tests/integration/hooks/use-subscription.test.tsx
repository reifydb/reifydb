// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, afterEach, describe, expect, it} from 'vitest';
import {renderHook, waitFor, act} from '@testing-library/react';
import {useSubscription, ConnectionProvider, getConnection, clearConnection, Schema} from '../../../src';
import {waitForDatabase} from '../setup';
import {createTestTableForHook} from './subscription-test-helpers';
// @ts-ignore
import React from 'react';

describe('useSubscription Hook', () => {
    const wrapper = ({children}: { children: React.ReactNode }) => (
        <ConnectionProvider config={{url: 'ws://127.0.0.1:8090'}} children={children}/>
    );

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    afterEach(async () => {
        await clearConnection();
        // Give connections time to clean up
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        await clearConnection();
    });

    describe('Auto-subscribe Behavior', () => {
        it('should auto-subscribe on mount when enabled=true (default)', async () => {
            const tableName = await createTestTableForHook(
                'auto_sub',
                ['id Int4', 'name Utf8']
            );

            const schema = Schema.object({id: Schema.number(), name: Schema.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema
                ),
                {wrapper}
            );

            // Should start subscribing
            expect(result.current.isSubscribing || result.current.isSubscribed).toBe(true);

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            expect(result.current.isSubscribing).toBe(false);
            expect(result.current.error).toBeUndefined();
            expect(result.current.subscriptionId).toBeDefined();
        });

        it('should not subscribe when enabled=false', async () => {
            const tableName = await createTestTableForHook(
                'no_auto_sub',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema,
                    {enabled: false}
                ),
                {wrapper}
            );

            // Give it a moment to potentially subscribe (it shouldn't)
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(result.current.isSubscribed).toBe(false);
            expect(result.current.isSubscribing).toBe(false);
            expect(result.current.subscriptionId).toBeUndefined();
        });

        it('should subscribe when enabled switches from false to true', async () => {
            const tableName = await createTestTableForHook(
                'enable_toggle',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema,
                    {enabled}
                ),
                {initialProps: {enabled: false}, wrapper}
            );

            expect(result.current.isSubscribed).toBe(false);

            // Enable subscription
            rerender({enabled: true});

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            expect(result.current.subscriptionId).toBeDefined();
        });

        it('should unsubscribe when enabled switches from true to false', async () => {
            const tableName = await createTestTableForHook(
                'disable_toggle',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema,
                    {enabled}
                ),
                {initialProps: {enabled: true}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            const subscriptionId = result.current.subscriptionId;
            expect(subscriptionId).toBeDefined();

            // Disable subscription
            rerender({enabled: false});

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(false);
            });

            expect(result.current.subscriptionId).toBeUndefined();
        });

        it('should unsubscribe on unmount', async () => {
            const tableName = await createTestTableForHook(
                'unmount_cleanup',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, unmount} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            // Unmount should trigger cleanup
            unmount();

            // Give cleanup time to execute
            await new Promise(resolve => setTimeout(resolve, 100));
        });
    });

    describe('Dependency Re-subscription', () => {
        it('should re-subscribe when query changes', async () => {
            const table1 = await createTestTableForHook(
                'query_change1',
                ['id Int4']
            );
            const table2 = await createTestTableForHook(
                'query_change2',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, rerender} = renderHook(
                ({query}) => useSubscription(
                    query,
                    null,
                    schema
                ),
                {initialProps: {query: `from test::${table1}`}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            const firstSubId = result.current.subscriptionId;

            // Change query
            rerender({query: `from test::${table2}`});

            await waitFor(() => {
                expect(result.current.subscriptionId).not.toBe(firstSubId);
            });

            expect(result.current.isSubscribed).toBe(true);
        });

        it('should re-subscribe when params change', async () => {
            const tableName = await createTestTableForHook(
                'params_change',
                ['id Int4', 'value Int4']
            );

            const schema = Schema.object({id: Schema.number(), value: Schema.number()});
            const {result, rerender} = renderHook(
                ({params}) => useSubscription(
                    `from test::${tableName} filter value == $val`,
                    params,
                    schema
                ),
                {initialProps: {params: {val: 1}}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            const firstSubId = result.current.subscriptionId;

            // Change params
            rerender({params: {val: 2}});

            await waitFor(() => {
                expect(result.current.subscriptionId).not.toBe(firstSubId);
            });

            expect(result.current.isSubscribed).toBe(true);
        });

        it('should re-subscribe when schema changes', async () => {
            const tableName = await createTestTableForHook(
                'schema_change',
                ['id Int4', 'name Utf8', 'value Int4']
            );

            const schema1 = Schema.object({id: Schema.number(), name: Schema.string()});
            const schema2 = Schema.object({id: Schema.number(), value: Schema.number()});

            const {result, rerender} = renderHook(
                ({schema}) => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema as any
                ),
                {initialProps: {schema: schema1}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            const firstSubId = result.current.subscriptionId;

            // Change schema
            rerender({schema: schema2});

            await waitFor(() => {
                expect(result.current.subscriptionId).not.toBe(firstSubId);
            });

            expect(result.current.isSubscribed).toBe(true);
        });

        it('should get new subscriptionId after re-subscription', async () => {
            const table1 = await createTestTableForHook(
                'resub_id1',
                ['id Int4']
            );
            const table2 = await createTestTableForHook(
                'resub_id2',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, rerender} = renderHook(
                ({query}) => useSubscription(
                    query,
                    null,
                    schema
                ),
                {initialProps: {query: `from test::${table1}`}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            const firstSubId = result.current.subscriptionId;
            expect(firstSubId).toBeDefined();

            // Change query to trigger re-subscription
            rerender({query: `from test::${table2}`});

            await waitFor(() => {
                const newSubId = result.current.subscriptionId;
                expect(newSubId).toBeDefined();
                expect(newSubId).not.toBe(firstSubId);
            });
        });
    });

    describe('ConnectionProvider Integration', () => {
        it('should work with ConnectionProvider wrapper', async () => {
            const tableName = await createTestTableForHook(
                'provider_test',
                ['id Int4', 'name Utf8']
            );

            const schema = Schema.object({id: Schema.number(), name: Schema.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            // Trigger an INSERT
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} FROM [{id: 1, name: 'test'}]`,
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
            const tableName = await createTestTableForHook(
                'config_override',
                ['id Int4']
            );

            const overrideConfig = {url: 'ws://127.0.0.1:8090', options: {timeoutMs: 2000}};
            const schema = Schema.object({id: Schema.number()});

            const {result, unmount} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema,
                    {connectionConfig: overrideConfig}
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            expect(result.current.error).toBeUndefined();

            // Clean up
            unmount();
            await clearConnection();
        });

        it('should flatten state from executor correctly', async () => {
            const tableName = await createTestTableForHook(
                'flatten_state',
                ['id Int4', 'value Utf8']
            );

            const schema = Schema.object({id: Schema.number(), value: Schema.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            // Check that all state properties are accessible at top level
            expect(result.current.data).toBeDefined();
            expect(result.current.changes).toBeDefined();
            expect(result.current.isSubscribed).toBe(true);
            expect(result.current.isSubscribing).toBe(false);
            expect(result.current.error).toBeUndefined();
            expect(result.current.subscriptionId).toBeDefined();
        });
    });

    describe('Operation Tracking', () => {
        it('should track INSERT operations', async () => {
            const tableName = await createTestTableForHook(
                'track_insert',
                ['id Int4', 'name Utf8']
            );

            const schema = Schema.object({id: Schema.number(), name: Schema.string()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(
                    `INSERT test::${tableName} FROM [{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]`,
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
            const tableName = await createTestTableForHook(
                'rapid_toggle',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result, rerender} = renderHook(
                ({enabled}) => useSubscription(
                    `from test::${tableName}`,
                    null,
                    schema,
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
                expect(result.current.isSubscribed).toBe(true);
            });

            expect(result.current.error).toBeUndefined();
        });

        it('should handle empty result sets', async () => {
            const tableName = await createTestTableForHook(
                'empty_results',
                ['id Int4']
            );

            const schema = Schema.object({id: Schema.number()});
            const {result} = renderHook(
                () => useSubscription(
                    `from test::${tableName} filter id > 1000`,
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.isSubscribed).toBe(true);
            });

            expect(result.current.data).toEqual([]);
            expect(result.current.changes).toEqual([]);
        });

        it('should handle subscription errors', async () => {
            const schema = Schema.object({id: Schema.number()});
            const {result} = renderHook(
                () => useSubscription(
                    'from nonexistent::table',
                    null,
                    schema
                ),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.isSubscribed).toBe(false);
        });
    });
});
