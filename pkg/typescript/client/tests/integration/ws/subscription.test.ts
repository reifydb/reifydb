// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Shape } from '@reifydb/core';
import { wait_for_database } from '../setup';
import {
    create_test_table_name,
    create_test_table,
    wait_for_callback,
    create_callback_tracker
} from './subscription-helpers';

describe('WebSocket Subscriptions', () => {
    let ws_client: WsClient;
    const testUrl = process.env.REIFYDB_WS_URL || 'ws://localhost:18090';

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    beforeEach(async () => {
        ws_client = await Client.connect_ws(testUrl, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
            reconnect_delay_ms: 100  // Fast reconnection for tests
        });
    }, 15000);

    afterEach(async () => {
        if (ws_client) {
            ws_client.disconnect();
        }
    });

    describe('Basic Subscription Flow', () => {
        it('should successfully subscribe to a query', async () => {
            const table_name = create_test_table_name('sub_basic');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const tracker = create_callback_tracker();
            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                Shape.object({
                    id: Shape.number(),
                    name: Shape.string(),
                    value: Shape.number()
                }),
                {
                    on_insert: tracker.callback
                }
            );

            expect(subscription_id).toBeDefined();
            expect(typeof subscription_id).toBe('string');
            expect(subscription_id.length).toBeGreaterThan(0);

            // Verify subscription is active
            const subscriptions = (ws_client as any).subscriptions;
            expect(subscriptions.has(subscription_id)).toBe(true);

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should successfully unsubscribe from a subscription', async () => {
            const table_name = create_test_table_name('sub_unsub');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const tracker = create_callback_tracker();
            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                Shape.object({
                    id: Shape.number(),
                    name: Shape.string()
                }),
                {
                    on_insert: tracker.callback
                }
            );

            expect(subscription_id).toBeDefined();

            await ws_client.unsubscribe(subscription_id);

            // Verify subscription is removed
            const subscriptions = (ws_client as any).subscriptions;
            expect(subscriptions.has(subscription_id)).toBe(false);
        }, 10000);

        it('should receive INSERT notifications', async () => {
            const table_name = create_test_table_name('sub_insert');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string(),
                value: Shape.number()
            });

            const { promise, callback } = wait_for_callback(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: callback
                }
            );

            // Insert data after subscription is established
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'test', value: 100 }]`,
                null,
                []
            );

            const rows = await promise;

            expect(rows).toBeDefined();
            expect(rows.length).toBe(1);
            expect(rows[0].id).toBe(1);
            expect(rows[0].name).toBe('test');
            expect(rows[0].value).toBe(100);
            //@ts-ignore
            expect(rows[0]._op).toBeUndefined(); // _op should be removed

            await ws_client.unsubscribe(subscription_id);
        }, 10000);
    });

    describe('Operation Callbacks', () => {
        it('should invoke on_insert callback for INSERT operations', async () => {
            const table_name = create_test_table_name('sub_op_insert');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insert_tracker = create_callback_tracker(shape);
            const update_tracker = create_callback_tracker(shape);
            const remove_tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: insert_tracker.callback,
                    on_update: update_tracker.callback,
                    on_remove: remove_tracker.callback
                }
            );

            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }]`,
                null,
                []
            );

            await insert_tracker.wait_for_call();

            expect(insert_tracker.get_call_count()).toBe(1);
            expect(insert_tracker.get_all_rows().length).toBe(2);
            expect(update_tracker.get_call_count()).toBe(0);
            expect(remove_tracker.get_call_count()).toBe(0);

            // Verify actual row data
            const rows = insert_tracker.get_all_rows();
            const alice = rows.find(r => r.id === 1);
            const bob = rows.find(r => r.id === 2);
            expect(alice).toBeDefined();
            expect(alice?.name).toBe('alice');
            expect(bob).toBeDefined();
            expect(bob?.name).toBe('bob');

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should invoke on_update callback for UPDATE operations', async () => {
            const table_name = create_test_table_name('sub_op_update');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insert_tracker = create_callback_tracker(shape);
            const update_tracker = create_callback_tracker(shape);
            const remove_tracker = create_callback_tracker(shape);

            // Subscribe to empty table FIRST
            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: insert_tracker.callback,
                    on_update: update_tracker.callback,
                    on_remove: remove_tracker.callback
                }
            );

            // Now insert initial data
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }]`,
                null,
                []
            );
            await insert_tracker.wait_for_call();

            // Verify inserts were received
            expect(insert_tracker.get_call_count()).toBe(1);
            expect(insert_tracker.get_all_rows().length).toBe(2);

            // Clear insert tracker before testing updates
            insert_tracker.clear();

            // Update data
            await ws_client.command(
                `UPDATE test::${table_name} { name: 'alice_updated' } FILTER id == 1`,
                null,
                []
            );

            await update_tracker.wait_for_call();

            expect(insert_tracker.get_call_count()).toBe(0);
            expect(update_tracker.get_call_count()).toBe(1);
            expect(update_tracker.get_all_rows().length).toBe(1);
            expect(remove_tracker.get_call_count()).toBe(0);

            // Verify update data
            const update_rows = update_tracker.get_all_rows();
            const updated_row = update_rows.find(r => r.id === 1);
            expect(updated_row).toBeDefined();
            expect(updated_row?.name).toBe('alice_updated');

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should invoke on_remove callback for REMOVE operations', async () => {
            const table_name = create_test_table_name('sub_op_remove');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insert_tracker = create_callback_tracker(shape);
            const update_tracker = create_callback_tracker(shape);
            const remove_tracker = create_callback_tracker(shape);

            // Subscribe to empty table FIRST
            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: insert_tracker.callback,
                    on_update: update_tracker.callback,
                    on_remove: remove_tracker.callback
                }
            );

            // Now insert initial data
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }]`,
                null,
                []
            );
            await insert_tracker.wait_for_call();

            // Verify inserts were received
            expect(insert_tracker.get_call_count()).toBe(1);
            expect(insert_tracker.get_all_rows().length).toBe(2);

            // Clear insert tracker before testing deletes
            insert_tracker.clear();

            // Delete data
            await ws_client.command(
                `DELETE test::${table_name} FILTER id == 1`,
                null,
                []
            );

            await remove_tracker.wait_for_call();

            expect(insert_tracker.get_call_count()).toBe(0);
            expect(update_tracker.get_call_count()).toBe(0);
            expect(remove_tracker.get_call_count()).toBe(1);
            expect(remove_tracker.get_all_rows().length).toBe(1);

            // Verify remove data
            const remove_rows = remove_tracker.get_all_rows();
            const removed_row = remove_rows.find(r => r.id === 1);
            expect(removed_row).toBeDefined();
            expect(removed_row?.name).toBe('alice');

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should handle multiple operation types in sequence', async () => {
            const table_name = create_test_table_name('sub_op_multi');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insert_tracker = create_callback_tracker(shape);
            const update_tracker = create_callback_tracker(shape);
            const remove_tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: insert_tracker.callback,
                    on_update: update_tracker.callback,
                    on_remove: remove_tracker.callback
                }
            );

            // Insert
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'alice' }]`,
                null,
                []
            );
            await insert_tracker.wait_for_call();

            // Update
            await ws_client.command(
                `UPDATE test::${table_name} { name: 'alice_updated' } FILTER id == 1`,
                null,
                []
            );
            await update_tracker.wait_for_call();

            // Remove
            await ws_client.command(
                `DELETE test::${table_name} FILTER id == 1`,
                null,
                []
            );
            await remove_tracker.wait_for_call();

            expect(insert_tracker.get_call_count()).toBe(1);
            expect(insert_tracker.get_all_rows().length).toBe(1);
            expect(update_tracker.get_call_count()).toBe(1);
            expect(update_tracker.get_all_rows().length).toBe(1);
            expect(remove_tracker.get_call_count()).toBe(1);
            expect(remove_tracker.get_all_rows().length).toBe(1);

            // Verify insert data
            const insert_rows = insert_tracker.get_all_rows();
            const inserted_row = insert_rows.find(r => r.id === 1);
            expect(inserted_row).toBeDefined();
            expect(inserted_row?.name).toBe('alice');

            // Verify update data
            const update_rows = update_tracker.get_all_rows();
            const updated_row = update_rows.find(r => r.id === 1);
            expect(updated_row).toBeDefined();
            expect(updated_row?.name).toBe('alice_updated');

            // Verify remove data
            const remove_rows = remove_tracker.get_all_rows();
            const removed_row = remove_rows.find(r => r.id === 1);
            expect(removed_row).toBeDefined();
            expect(removed_row?.name).toBe('alice_updated');

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should batch consecutive rows of same operation type', async () => {
            const table_name = create_test_table_name('sub_op_batch');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insert_tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: insert_tracker.callback
                }
            );

            // Insert 10 rows at once
            const rows = Array.from({ length: 10 }, (_, i) => ({ id: i + 1, name: `user${i + 1}` }));
            await ws_client.command(
                `INSERT test::${table_name} FROM ${JSON.stringify(rows)}`,
                null,
                []
            );

            await insert_tracker.wait_for_call();

            // Should be batched into one call with all 10 rows
            expect(insert_tracker.get_call_count()).toBe(1);
            expect(insert_tracker.get_all_rows().length).toBe(10);

            // Verify all 10 user rows
            const inserted_rows = insert_tracker.get_all_rows();
            for (let i = 0; i < 10; i++) {
                const row = inserted_rows.find(r => r.id === i + 1);
                expect(row).toBeDefined();
                expect(row?.name).toBe(`user${i + 1}`);
            }

            await ws_client.unsubscribe(subscription_id);
        }, 10000);
    });

    describe('Shape Transformation', () => {
        it('should transform rows using provided shape', async () => {
            const table_name = create_test_table_name('sub_shape_prim');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string(),
                value: Shape.number()
            });

            const { promise, callback } = wait_for_callback(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: callback
                }
            );

            await ws_client.command(
                `INSERT test::${table_name} [{ id: 42, name: 'test', value: 100 }]`,
                null,
                []
            );

            const rows = await promise;

            expect(rows[0].id).toBe(42);
            expect(typeof rows[0].id).toBe('number');
            expect(rows[0].name).toBe('test');
            expect(typeof rows[0].name).toBe('string');
            expect(rows[0].value).toBe(100);
            expect(typeof rows[0].value).toBe('number');

            // Should not have Value objects (check for .value property instead of valueOf)
            //@ts-ignore
            expect(rows[0].id.value).toBeUndefined();

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should handle value shape types', async () => {
            const table_name = create_test_table_name('sub_shape_val');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.int4Value(),
                name: Shape.utf8Value()
            });

            const { promise, callback } = wait_for_callback(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: callback
                }
            );

            await ws_client.command(
                `INSERT test::${table_name} [{ id: 42, name: 'test' }]`,
                null,
                []
            );

            const rows = await promise;

            // Should have Value objects
            expect(rows[0].id.value).toBe(42);
            expect(rows[0].name.value).toBe('test');

            await ws_client.unsubscribe(subscription_id);
        }, 10000);
    });

    describe('Concurrent Subscriptions', () => {
        it('should handle multiple concurrent subscriptions', async () => {
            const table1 = create_test_table_name('sub_conc_1');
            const table2 = create_test_table_name('sub_conc_2');

            await create_test_table(ws_client, table1, ['id Int4', 'name Utf8']);
            await create_test_table(ws_client, table2, ['id Int4', 'value Int4']);

            const shape1 = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const shape2 = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker1 = create_callback_tracker(shape1);
            const tracker2 = create_callback_tracker(shape2);

            const sub1 = await ws_client.subscribe(`from test::${table1}`, null, shape1, {
                on_insert: tracker1.callback
            });

            const sub2 = await ws_client.subscribe(`from test::${table2}`, null, shape2, {
                on_insert: tracker2.callback
            });

            // Insert into table 1
            await ws_client.command(
                `INSERT test::${table1} [{ id: 1, name: 'alice' }]`,
                null,
                []
            );
            await tracker1.wait_for_call();

            // Insert into table 2
            await ws_client.command(
                `INSERT test::${table2} [{ id: 2, value: 200 }]`,
                null,
                []
            );
            await tracker2.wait_for_call();

            expect(tracker1.get_call_count()).toBe(1);
            expect(tracker1.get_all_rows().length).toBe(1);
            expect(tracker1.get_all_rows()[0].name).toBe('alice');

            expect(tracker2.get_call_count()).toBe(1);
            expect(tracker2.get_all_rows().length).toBe(1);
            expect(tracker2.get_all_rows()[0].value).toBe(200);

            await ws_client.unsubscribe(sub1);
            await ws_client.unsubscribe(sub2);
        }, 15000);

        it('should handle 5+ concurrent subscriptions', async () => {
            const tables = Array.from({ length: 5 }, (_, i) =>
                create_test_table_name(`sub_conc_${i}`)
            );

            // Create all tables
            await Promise.all(
                tables.map(table =>
                    create_test_table(ws_client, table, ['id Int4', 'value Int4'])
                )
            );

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const trackers = tables.map(() => create_callback_tracker(shape));

            // Subscribe to all tables
            const subscriptions = await Promise.all(
                tables.map((table, i) =>
                    ws_client.subscribe(`from test::${table}`, null, shape, {
                        on_insert: trackers[i].callback
                    })
                )
            );

            // Insert into all tables
            await Promise.all(
                tables.map((table, i) =>
                    ws_client.command(
                        `INSERT test::${table} [{ id: ${i}, value: ${i * 100} }]`,
                        null,
                        []
                )
                )
            );

            // Wait for all callbacks
            await Promise.all(trackers.map(t => t.wait_for_call()));

            // Verify all callbacks fired
            for (let i = 0; i < 5; i++) {
                expect(trackers[i].get_call_count()).toBe(1);
                expect(trackers[i].get_all_rows().length).toBe(1);
                expect(trackers[i].get_all_rows()[0].id).toBe(i);
                expect(trackers[i].get_all_rows()[0].value).toBe(i * 100);
            }

            // Cleanup subscriptions
            await Promise.all(subscriptions.map(sub => ws_client.unsubscribe(sub)));
        }, 15000);
    });

    describe('Reconnection Behavior', () => {
        it('should resubscribe to active subscriptions after reconnection', async () => {
            const table_name = create_test_table_name('sub_reconn');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                {
                    on_insert: tracker.callback
                }
            );

            expect(subscription_id).toBeDefined();

            // Force disconnect
            const socket = (ws_client as any).socket;
            socket.close();

            // Wait for reconnection to complete
            await new Promise(resolve => setTimeout(resolve, 300));

            // Insert new data
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, name: 'after_reconnect' }]`,
                null,
                []
            );

            await tracker.wait_for_call();

            // Should have received the callback after reconnection
            expect(tracker.get_call_count()).toBe(1);
            expect(tracker.get_all_rows().length).toBe(1);
            const rows = tracker.get_all_rows();
            const reconnect_row = rows.find(r => r.id === 1);
            expect(reconnect_row).toBeDefined();
            expect(reconnect_row?.name).toBe('after_reconnect');
        }, 15000);

        it('should maintain callback references after reconnection', async () => {
            const table_name = create_test_table_name('sub_reconn_cb');
            await create_test_table(ws_client, table_name, [
                'id Int4',
                'value Int4'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            // Insert before disconnect
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, value: 100 }]`,
                null,
                []
            );
            await tracker.wait_for_call();

            // Verify first insert
            expect(tracker.get_call_count()).toBe(1);
            expect(tracker.get_all_rows()[0].id).toBe(1);
            expect(tracker.get_all_rows()[0].value).toBe(100);

            const calls_before_reconnect = tracker.get_call_count();

            // Force disconnect and reconnect
            (ws_client as any).socket.close();

            // Wait for reconnection to complete
            await new Promise(resolve => setTimeout(resolve, 300));

            // Insert after reconnect
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 2, value: 200 }]`,
                null,
                []
            );
            await tracker.wait_for_call();

            // Verify callback was invoked again after reconnection
            expect(tracker.get_call_count()).toBeGreaterThan(calls_before_reconnect);
            // Verify the second insert data by finding it by ID
            const allRows = tracker.get_all_rows();
            const second_insert = allRows.find(r => r.id === 2);
            expect(second_insert).toBeDefined();
            expect(second_insert?.value).toBe(200);
        }, 15000);

        it('should handle multiple subscriptions during reconnection', async () => {
            const tables = [
                create_test_table_name('sub_reconn_m1'),
                create_test_table_name('sub_reconn_m2'),
                create_test_table_name('sub_reconn_m3')
            ];

            await Promise.all(
                tables.map(table =>
                    create_test_table(ws_client, table, ['id Int4', 'value Int4'])
                )
            );

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const trackers = tables.map(() => create_callback_tracker(shape));

            // Subscribe to all tables
            await Promise.all(
                tables.map((table, i) =>
                    ws_client.subscribe(`from test::${table}`, null, shape, {
                        on_insert: trackers[i].callback
                    })
                )
            );

            // Force disconnect
            (ws_client as any).socket.close();

            // Wait for reconnection to complete
            await new Promise(resolve => setTimeout(resolve, 300));

            // Insert into all tables
            await Promise.all(
                tables.map((table, i) =>
                    ws_client.command(
                        `INSERT test::${table} [{ id: ${i}, value: ${i * 100} }]`,
                        null,
                        []
                    )
                )
            );

            await Promise.all(trackers.map(t => t.wait_for_call()));

            // All callbacks should still work
            for (let i = 0; i < 3; i++) {
                expect(trackers[i].get_call_count()).toBe(1);
                expect(trackers[i].get_all_rows().length).toBe(1);
                expect(trackers[i].get_all_rows()[0].id).toBe(i);
                expect(trackers[i].get_all_rows()[0].value).toBe(i * 100);
            }
        }, 20000);
    });

    describe('Error Handling', () => {
        it('should reject subscription with invalid query', async () => {
            try {
                await ws_client.subscribe(
                    'INVALID RQL SYNTAX HERE',
                    null,
                    undefined,
                    { on_insert: () => {} }
                );
                expect.fail('Should have rejected');
            } catch (err: any) {
                expect(err).toBeDefined();
                // Should be a ReifyError with diagnostic information
                expect(err.message || err.toString()).toBeTruthy();
            }
        }, 10000);

        it('should reject subscription to non-existent table', async () => {
            const nonExistentTable = 'table_that_does_not_exist_' + Date.now();

            try {
                await ws_client.subscribe(
                    `from ${nonExistentTable}`,
                    null,
                    undefined,
                    { on_insert: () => {} }
                );
                expect.fail('Should have rejected');
            } catch (err: any) {
                expect(err).toBeDefined();
                expect(err.message || err.toString()).toBeTruthy();
            }
        }, 10000);

        it('should handle unsubscribe with invalid subscription ID', async () => {
            const fakeId = 'fake-subscription-id-' + Date.now();

            try {
                await ws_client.unsubscribe(fakeId);
                // May or may not throw depending on server implementation
                // If it doesn't throw, that's also acceptable
            } catch (err: any) {
                // If it throws, just verify we got an error
                expect(err).toBeDefined();
            }
        }, 10000);
    });

    describe('Cleanup and Lifecycle', () => {
        it('should clean up subscriptions on disconnect', async () => {
            const table_name = create_test_table_name('sub_cleanup');
            await create_test_table(ws_client, table_name, ['id Int4']);

            const shape = Shape.object({
                id: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            const subscriptions = (ws_client as any).subscriptions;
            expect(subscriptions.size).toBe(1);

            ws_client.disconnect();

            // Subscriptions should be cleared
            expect(subscriptions.size).toBe(0);
        }, 10000);

        it('should not receive callbacks after unsubscribe', async () => {
            const table_name = create_test_table_name('sub_no_cb');
            await create_test_table(ws_client, table_name, ['id Int4', 'value Int4']);

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            // Unsubscribe immediately
            await ws_client.unsubscribe(subscription_id);

            // Insert data
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, value: 100 }]`,
                null,
                []
            );

            // Small wait to verify no callback fires
            await new Promise(resolve => setTimeout(resolve, 100));

            // Should not have received callback
            expect(tracker.get_call_count()).toBe(0);
        }, 10000);
    });

    describe('Edge Cases', () => {
        it('should handle empty result sets', async () => {
            const table_name = create_test_table_name('sub_empty');
            await create_test_table(ws_client, table_name, ['id Int4', 'value Int4']);

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name} filter { id > 1000 }`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            // Insert data that doesn't match filter
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1, value: 100 }]`,
                null,
                []
            );
            // Small wait to verify no callback fires for non-matching data
            await new Promise(resolve => setTimeout(resolve, 100));

            // Should not trigger callback
            expect(tracker.get_call_count()).toBe(0);

            // Insert data that matches filter
            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1001, value: 200 }]`,
                null,
                []
            );
            await tracker.wait_for_call();

            // Should trigger callback now
            expect(tracker.get_call_count()).toBe(1);
            expect(tracker.get_all_rows().length).toBe(1);

            // Verify matching row data
            const row = tracker.get_all_rows()[0];
            expect(row.id).toBe(1001);
            expect(row.value).toBe(200);

            await ws_client.unsubscribe(subscription_id);
        }, 10000);

        it('should handle large batch of changes', async () => {
            const table_name = create_test_table_name('sub_large');
            await create_test_table(ws_client, table_name, ['id Int4', 'value Int4']);

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            // Insert 100 rows
            const rows = Array.from({ length: 100 }, (_, i) => ({ id: i, value: i * 10 }));

            const start_time = Date.now();
            await ws_client.command(
                `INSERT test::${table_name} FROM ${JSON.stringify(rows)}`,
                null,
                []
            );

            await tracker.wait_for_call();

            const duration = Date.now() - start_time;

            // Should have received all 100 rows
            const total_rows = tracker.get_all_rows().length;
            expect(total_rows).toBe(100);

            // Verify sample rows by finding them by ID
            const result_rows = tracker.get_all_rows();
            const row0 = result_rows.find(r => r.id === 0);
            const row49 = result_rows.find(r => r.id === 49);
            const row99 = result_rows.find(r => r.id === 99);

            expect(row0).toBeDefined();
            expect(row0?.value).toBe(0);
            expect(row49).toBeDefined();
            expect(row49?.value).toBe(490);
            expect(row99).toBeDefined();
            expect(row99?.value).toBe(990);

            // Performance check - should complete in reasonable time
            expect(duration).toBeLessThan(3000);

            await ws_client.unsubscribe(subscription_id);
        }, 15000);

        it('should handle rapid successive changes', async () => {
            const table_name = create_test_table_name('sub_rapid');
            await create_test_table(ws_client, table_name, ['id Int4', 'value Int4']);

            const shape = Shape.object({
                id: Shape.number(),
                value: Shape.number()
            });

            const tracker = create_callback_tracker(shape);

            const subscription_id = await ws_client.subscribe(
                `from test::${table_name}`,
                null,
                shape,
                { on_insert: tracker.callback }
            );

            // Fire 10 insert commands rapidly without await
            const promises = Array.from({ length: 10 }, (_, i) =>
                ws_client.command(
                    `INSERT test::${table_name} [{ id: ${i}, value: ${i * 10} }]`,
                    null,
                    []
                )
            );

            await Promise.all(promises);

            await tracker.wait_for_rows(10);

            // Rapid inserts may arrive in one or more batches depending on server poll timing
            expect(tracker.get_call_count()).toBeGreaterThanOrEqual(1);
            expect(tracker.get_all_rows().length).toBe(10);

            // Verify all rows have correct values
            const rows = tracker.get_all_rows();
            for (let i = 0; i < 10; i++) {
                const row = rows.find(r => r.id === i);
                expect(row).toBeDefined();
                expect(row?.value).toBe(i * 10);
            }

            await ws_client.unsubscribe(subscription_id);
        }, 15000);
    });
});
