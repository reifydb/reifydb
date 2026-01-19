// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Schema } from '@reifydb/core';
import { waitForDatabase } from '../setup';
import {
    createTestTableName,
    createTestTable,
    waitForCallback,
    createCallbackTracker
} from './subscription-helpers';

describe('WebSocket Subscriptions', () => {
    let wsClient: WsClient;
    const testUrl = process.env.REIFYDB_WS_URL || 'ws://localhost:8090';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        wsClient = await Client.connect_ws(testUrl, {
            timeoutMs: 10000,
            reconnectDelayMs: 100  // Fast reconnection for tests
        });
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            wsClient.disconnect();
        }
    });

    describe('Basic Subscription Flow', () => {
        it('should successfully subscribe to a query', async () => {
            const tableName = createTestTableName('sub_basic');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const tracker = createCallbackTracker();
            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                Schema.object({
                    id: Schema.number(),
                    name: Schema.string(),
                    value: Schema.number()
                }),
                {
                    onInsert: tracker.callback
                }
            );

            expect(subscriptionId).toBeDefined();
            expect(typeof subscriptionId).toBe('string');
            expect(subscriptionId.length).toBeGreaterThan(0);

            // Verify subscription is active
            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.has(subscriptionId)).toBe(true);

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should successfully unsubscribe from a subscription', async () => {
            const tableName = createTestTableName('sub_unsub');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const tracker = createCallbackTracker();
            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                Schema.object({
                    id: Schema.number(),
                    name: Schema.string()
                }),
                {
                    onInsert: tracker.callback
                }
            );

            expect(subscriptionId).toBeDefined();

            await wsClient.unsubscribe(subscriptionId);

            // Verify subscription is removed
            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.has(subscriptionId)).toBe(false);
        }, 10000);

        it('should receive INSERT notifications', async () => {
            const tableName = createTestTableName('sub_insert');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string(),
                value: Schema.number()
            });

            const { promise, callback } = waitForCallback(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: callback
                }
            );

            // Insert data after subscription is established
            await wsClient.command(
                `from [{ id: 1, name: 'test', value: 100 }] insert test.${tableName}`,
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

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);
    });

    describe('Operation Callbacks', () => {
        it('should invoke onInsert callback for INSERT operations', async () => {
            const tableName = createTestTableName('sub_op_insert');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const insertTracker = createCallbackTracker(schema);
            const updateTracker = createCallbackTracker(schema);
            const removeTracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: insertTracker.callback,
                    onUpdate: updateTracker.callback,
                    onRemove: removeTracker.callback
                }
            );

            await wsClient.command(
                `from [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }] insert test.${tableName}`,
                null,
                []
            );

            // Wait for callbacks
            await new Promise(resolve => setTimeout(resolve, 500));

            expect(insertTracker.getCallCount()).toBe(1);
            expect(insertTracker.getAllRows().length).toBe(2);
            expect(updateTracker.getCallCount()).toBe(0);
            expect(removeTracker.getCallCount()).toBe(0);

            // Verify actual row data
            const rows = insertTracker.getAllRows();
            const alice = rows.find(r => r.id === 1);
            const bob = rows.find(r => r.id === 2);
            expect(alice).toBeDefined();
            expect(alice?.name).toBe('alice');
            expect(bob).toBeDefined();
            expect(bob?.name).toBe('bob');

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should invoke onUpdate callback for UPDATE operations', async () => {
            const tableName = createTestTableName('sub_op_update');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const insertTracker = createCallbackTracker(schema);
            const updateTracker = createCallbackTracker(schema);
            const removeTracker = createCallbackTracker(schema);

            // Subscribe to empty table FIRST
            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: insertTracker.callback,
                    onUpdate: updateTracker.callback,
                    onRemove: removeTracker.callback
                }
            );

            // Wait for subscription to be established
            await new Promise(resolve => setTimeout(resolve, 100));

            // Now insert initial data
            await wsClient.command(
                `from [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 200));

            // Verify inserts were received
            expect(insertTracker.getCallCount()).toBe(1);
            expect(insertTracker.getAllRows().length).toBe(2);

            // Clear insert tracker before testing updates
            insertTracker.clear();

            // Update data
            await wsClient.command(
                `from test.${tableName} filter { id == 1 } map { id: id, name: 'alice_updated' } update test.${tableName}`,
                null,
                []
            );

            // Wait for callbacks
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(insertTracker.getCallCount()).toBe(0);
            expect(updateTracker.getCallCount()).toBe(1);
            expect(updateTracker.getAllRows().length).toBe(1);
            expect(removeTracker.getCallCount()).toBe(0);

            // Verify update data
            const updateRows = updateTracker.getAllRows();
            const updatedRow = updateRows.find(r => r.id === 1);
            expect(updatedRow).toBeDefined();
            expect(updatedRow?.name).toBe('alice_updated');

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should invoke onRemove callback for REMOVE operations', async () => {
            const tableName = createTestTableName('sub_op_remove');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const insertTracker = createCallbackTracker(schema);
            const updateTracker = createCallbackTracker(schema);
            const removeTracker = createCallbackTracker(schema);

            // Subscribe to empty table FIRST
            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: insertTracker.callback,
                    onUpdate: updateTracker.callback,
                    onRemove: removeTracker.callback
                }
            );

            // Wait for subscription to be established
            await new Promise(resolve => setTimeout(resolve, 100));

            // Now insert initial data
            await wsClient.command(
                `from [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 200));

            // Verify inserts were received
            expect(insertTracker.getCallCount()).toBe(1);
            expect(insertTracker.getAllRows().length).toBe(2);

            // Clear insert tracker before testing deletes
            insertTracker.clear();

            // Delete data
            await wsClient.command(
                `from test.${tableName} filter { id == 1 } delete test.${tableName}`,
                null,
                []
            );

            // Wait for callbacks
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(insertTracker.getCallCount()).toBe(0);
            expect(updateTracker.getCallCount()).toBe(0);
            expect(removeTracker.getCallCount()).toBe(1);
            expect(removeTracker.getAllRows().length).toBe(1);

            // Verify remove data
            const removeRows = removeTracker.getAllRows();
            const removedRow = removeRows.find(r => r.id === 1);
            expect(removedRow).toBeDefined();
            expect(removedRow?.name).toBe('alice');

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should handle multiple operation types in sequence', async () => {
            const tableName = createTestTableName('sub_op_multi');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const insertTracker = createCallbackTracker(schema);
            const updateTracker = createCallbackTracker(schema);
            const removeTracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: insertTracker.callback,
                    onUpdate: updateTracker.callback,
                    onRemove: removeTracker.callback
                }
            );

            // Wait for subscription to be fully established
            await new Promise(resolve => setTimeout(resolve, 100));

            // Insert
            await wsClient.command(
                `from [{ id: 1, name: 'alice' }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 200));

            // Update
            await wsClient.command(
                `from test.${tableName} filter { id == 1 } map { id: id, name: 'alice_updated' } update test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 200));

            // Remove
            await wsClient.command(
                `from test.${tableName} filter { id == 1 } delete test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(insertTracker.getCallCount()).toBe(1);
            expect(insertTracker.getAllRows().length).toBe(1);
            expect(updateTracker.getCallCount()).toBe(1);
            expect(updateTracker.getAllRows().length).toBe(1);
            expect(removeTracker.getCallCount()).toBe(1);
            expect(removeTracker.getAllRows().length).toBe(1);

            // Verify insert data
            const insertRows = insertTracker.getAllRows();
            const insertedRow = insertRows.find(r => r.id === 1);
            expect(insertedRow).toBeDefined();
            expect(insertedRow?.name).toBe('alice');

            // Verify update data
            const updateRows = updateTracker.getAllRows();
            const updatedRow = updateRows.find(r => r.id === 1);
            expect(updatedRow).toBeDefined();
            expect(updatedRow?.name).toBe('alice_updated');

            // Verify remove data
            const removeRows = removeTracker.getAllRows();
            const removedRow = removeRows.find(r => r.id === 1);
            expect(removedRow).toBeDefined();
            expect(removedRow?.name).toBe('alice_updated');

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should batch consecutive rows of same operation type', async () => {
            const tableName = createTestTableName('sub_op_batch');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const insertTracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: insertTracker.callback
                }
            );

            // Insert 10 rows at once
            const rows = Array.from({ length: 10 }, (_, i) => ({ id: i + 1, name: `user${i + 1}` }));
            await wsClient.command(
                `from ${JSON.stringify(rows)} insert test.${tableName}`,
                null,
                []
            );

            // Wait for callbacks
            await new Promise(resolve => setTimeout(resolve, 500));

            // Should be batched into one call with all 10 rows
            expect(insertTracker.getCallCount()).toBe(1);
            expect(insertTracker.getAllRows().length).toBe(10);

            // Verify all 10 user rows
            const insertedRows = insertTracker.getAllRows();
            for (let i = 0; i < 10; i++) {
                const row = insertedRows.find(r => r.id === i + 1);
                expect(row).toBeDefined();
                expect(row?.name).toBe(`user${i + 1}`);
            }

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);
    });

    describe('Schema Transformation', () => {
        it('should transform rows using provided schema', async () => {
            const tableName = createTestTableName('sub_schema_prim');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8',
                'value Int4'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string(),
                value: Schema.number()
            });

            const { promise, callback } = waitForCallback(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: callback
                }
            );

            await wsClient.command(
                `from [{ id: 42, name: 'test', value: 100 }] insert test.${tableName}`,
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

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should handle value schema types', async () => {
            const tableName = createTestTableName('sub_schema_val');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.int4Value(),
                name: Schema.utf8Value()
            });

            const { promise, callback } = waitForCallback(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: callback
                }
            );

            await wsClient.command(
                `from [{ id: 42, name: 'test' }] insert test.${tableName}`,
                null,
                []
            );

            const rows = await promise;

            // Should have Value objects
            expect(rows[0].id.value).toBe(42);
            expect(rows[0].name.value).toBe('test');

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);
    });

    describe('Concurrent Subscriptions', () => {
        it('should handle multiple concurrent subscriptions', async () => {
            const table1 = createTestTableName('sub_conc_1');
            const table2 = createTestTableName('sub_conc_2');

            await createTestTable(wsClient, table1, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, table2, ['id Int4', 'value Int4']);

            const schema1 = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const schema2 = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker1 = createCallbackTracker(schema1);
            const tracker2 = createCallbackTracker(schema2);

            const sub1 = await wsClient.subscribe(`from test.${table1}`, null, schema1, {
                onInsert: tracker1.callback
            });

            const sub2 = await wsClient.subscribe(`from test.${table2}`, null, schema2, {
                onInsert: tracker2.callback
            });

            // Insert into table 1
            await wsClient.command(
                `from [{ id: 1, name: 'alice' }] insert test.${table1}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            // Insert into table 2
            await wsClient.command(
                `from [{ id: 2, value: 200 }] insert test.${table2}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            expect(tracker1.getCallCount()).toBe(1);
            expect(tracker1.getAllRows().length).toBe(1);
            expect(tracker1.getAllRows()[0].name).toBe('alice');

            expect(tracker2.getCallCount()).toBe(1);
            expect(tracker2.getAllRows().length).toBe(1);
            expect(tracker2.getAllRows()[0].value).toBe(200);

            await wsClient.unsubscribe(sub1);
            await wsClient.unsubscribe(sub2);
        }, 15000);

        it('should handle 5+ concurrent subscriptions', async () => {
            const tables = Array.from({ length: 5 }, (_, i) =>
                createTestTableName(`sub_conc_${i}`)
            );

            // Create all tables
            await Promise.all(
                tables.map(table =>
                    createTestTable(wsClient, table, ['id Int4', 'value Int4'])
                )
            );

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const trackers = tables.map(() => createCallbackTracker(schema));

            // Subscribe to all tables
            const subscriptions = await Promise.all(
                tables.map((table, i) =>
                    wsClient.subscribe(`from test.${table}`, null, schema, {
                        onInsert: trackers[i].callback
                    })
                )
            );

            // Wait for subscriptions to be established
            await new Promise(resolve => setTimeout(resolve, 100));

            // Insert into all tables
            await Promise.all(
                tables.map((table, i) =>
                    wsClient.command(
                        `from [{ id: ${i}, value: ${i * 100} }] insert test.${table}`,
                        null,
                        []
                )
                )
            );

            // Wait for all callbacks
            await new Promise(resolve => setTimeout(resolve, 500));

            // Verify all callbacks fired
            for (let i = 0; i < 5; i++) {
                expect(trackers[i].getCallCount()).toBe(1);
                expect(trackers[i].getAllRows().length).toBe(1);
                expect(trackers[i].getAllRows()[0].id).toBe(i);
                expect(trackers[i].getAllRows()[0].value).toBe(i * 100);
            }

            // Cleanup subscriptions
            await Promise.all(subscriptions.map(sub => wsClient.unsubscribe(sub)));
        }, 15000);
    });

    describe('Reconnection Behavior', () => {
        it('should resubscribe to active subscriptions after reconnection', async () => {
            const tableName = createTestTableName('sub_reconn');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                name: Schema.string()
            });

            const tracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                {
                    onInsert: tracker.callback
                }
            );

            expect(subscriptionId).toBeDefined();

            // Force disconnect
            const socket = (wsClient as any).socket;
            socket.close();

            // Wait for reconnection to complete
            await new Promise(resolve => setTimeout(resolve, 500));

            // Insert new data
            await wsClient.command(
                `from [{ id: 1, name: 'after_reconnect' }] insert test.${tableName}`,
                null,
                []
            );

            // Wait for callback
            await new Promise(resolve => setTimeout(resolve, 500));

            // Should have received the callback after reconnection
            expect(tracker.getCallCount()).toBe(1);
            expect(tracker.getAllRows().length).toBe(1);
            const rows = tracker.getAllRows();
            const reconnectRow = rows.find(r => r.id === 1);
            expect(reconnectRow).toBeDefined();
            expect(reconnectRow?.name).toBe('after_reconnect');
        }, 15000);

        it('should maintain callback references after reconnection', async () => {
            const tableName = createTestTableName('sub_reconn_cb');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'value Int4'
            ]);

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            // Insert before disconnect
            await wsClient.command(
                `from [{ id: 1, value: 100 }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            // Verify first insert
            expect(tracker.getCallCount()).toBe(1);
            expect(tracker.getAllRows()[0].id).toBe(1);
            expect(tracker.getAllRows()[0].value).toBe(100);

            const callsBeforeReconnect = tracker.getCallCount();

            // Force disconnect and reconnect
            (wsClient as any).socket.close();
            await new Promise(resolve => setTimeout(resolve, 500));

            // Insert after reconnect
            await wsClient.command(
                `from [{ id: 2, value: 200 }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            // Verify callback was invoked again after reconnection
            expect(tracker.getCallCount()).toBeGreaterThan(callsBeforeReconnect);
            // Verify the second insert data by finding it by ID
            const allRows = tracker.getAllRows();
            const secondInsert = allRows.find(r => r.id === 2);
            expect(secondInsert).toBeDefined();
            expect(secondInsert?.value).toBe(200);
        }, 15000);

        it('should handle multiple subscriptions during reconnection', async () => {
            const tables = [
                createTestTableName('sub_reconn_m1'),
                createTestTableName('sub_reconn_m2'),
                createTestTableName('sub_reconn_m3')
            ];

            await Promise.all(
                tables.map(table =>
                    createTestTable(wsClient, table, ['id Int4', 'value Int4'])
                )
            );

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const trackers = tables.map(() => createCallbackTracker(schema));

            // Subscribe to all tables
            await Promise.all(
                tables.map((table, i) =>
                    wsClient.subscribe(`from test.${table}`, null, schema, {
                        onInsert: trackers[i].callback
                    })
                )
            );

            // Force disconnect
            (wsClient as any).socket.close();
            await new Promise(resolve => setTimeout(resolve, 500));

            // Insert into all tables
            await Promise.all(
                tables.map((table, i) =>
                    wsClient.command(
                        `from [{ id: ${i}, value: ${i * 100} }] insert test.${table}`,
                        null,
                        []
                    )
                )
            );

            await new Promise(resolve => setTimeout(resolve, 500));

            // All callbacks should still work
            for (let i = 0; i < 3; i++) {
                expect(trackers[i].getCallCount()).toBe(1);
                expect(trackers[i].getAllRows().length).toBe(1);
                expect(trackers[i].getAllRows()[0].id).toBe(i);
                expect(trackers[i].getAllRows()[0].value).toBe(i * 100);
            }
        }, 20000);
    });

    describe('Error Handling', () => {
        it('should reject subscription with invalid query', async () => {
            try {
                await wsClient.subscribe(
                    'INVALID RQL SYNTAX HERE',
                    null,
                    undefined,
                    { onInsert: () => {} }
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
                await wsClient.subscribe(
                    `from ${nonExistentTable}`,
                    null,
                    undefined,
                    { onInsert: () => {} }
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
                await wsClient.unsubscribe(fakeId);
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
            const tableName = createTestTableName('sub_cleanup');
            await createTestTable(wsClient, tableName, ['id Int4']);

            const schema = Schema.object({
                id: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.size).toBe(1);

            wsClient.disconnect();

            // Subscriptions should be cleared
            expect(subscriptions.size).toBe(0);
        }, 10000);

        it('should not receive callbacks after unsubscribe', async () => {
            const tableName = createTestTableName('sub_no_cb');
            await createTestTable(wsClient, tableName, ['id Int4', 'value Int4']);

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            // Unsubscribe immediately
            await wsClient.unsubscribe(subscriptionId);

            // Insert data
            await wsClient.command(
                `from [{ id: 1, value: 100 }] insert test.${tableName}`,
                null,
                []
            );

            // Wait
            await new Promise(resolve => setTimeout(resolve, 500));

            // Should not have received callback
            expect(tracker.getCallCount()).toBe(0);
        }, 10000);
    });

    describe('Edge Cases', () => {
        it('should handle empty result sets', async () => {
            const tableName = createTestTableName('sub_empty');
            await createTestTable(wsClient, tableName, ['id Int4', 'value Int4']);

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName} filter { id > 1000 }`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            // Insert data that doesn't match filter
            await wsClient.command(
                `from [{ id: 1, value: 100 }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            // Should not trigger callback
            expect(tracker.getCallCount()).toBe(0);

            // Insert data that matches filter
            await wsClient.command(
                `from [{ id: 1001, value: 200 }] insert test.${tableName}`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            // Should trigger callback now
            expect(tracker.getCallCount()).toBe(1);
            expect(tracker.getAllRows().length).toBe(1);

            // Verify matching row data
            const row = tracker.getAllRows()[0];
            expect(row.id).toBe(1001);
            expect(row.value).toBe(200);

            await wsClient.unsubscribe(subscriptionId);
        }, 10000);

        it('should handle large batch of changes', async () => {
            const tableName = createTestTableName('sub_large');
            await createTestTable(wsClient, tableName, ['id Int4', 'value Int4']);

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            // Insert 100 rows
            const rows = Array.from({ length: 100 }, (_, i) => ({ id: i, value: i * 10 }));

            const startTime = Date.now();
            await wsClient.command(
                `from ${JSON.stringify(rows)} insert test.${tableName}`,
                null,
                []
            );

            // Wait for callback
            await new Promise(resolve => setTimeout(resolve, 2000));

            const duration = Date.now() - startTime;

            // Should have received all 100 rows
            const totalRows = tracker.getAllRows().length;
            expect(totalRows).toBe(100);

            // Verify sample rows by finding them by ID
            const resultRows = tracker.getAllRows();
            const row0 = resultRows.find(r => r.id === 0);
            const row49 = resultRows.find(r => r.id === 49);
            const row99 = resultRows.find(r => r.id === 99);

            expect(row0).toBeDefined();
            expect(row0?.value).toBe(0);
            expect(row49).toBeDefined();
            expect(row49?.value).toBe(490);
            expect(row99).toBeDefined();
            expect(row99?.value).toBe(990);

            // Performance check - should complete in reasonable time
            expect(duration).toBeLessThan(3000);

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);

        it('should handle rapid successive changes', async () => {
            const tableName = createTestTableName('sub_rapid');
            await createTestTable(wsClient, tableName, ['id Int4', 'value Int4']);

            const schema = Schema.object({
                id: Schema.number(),
                value: Schema.number()
            });

            const tracker = createCallbackTracker(schema);

            const subscriptionId = await wsClient.subscribe(
                `from test.${tableName}`,
                null,
                schema,
                { onInsert: tracker.callback }
            );

            // Fire 10 insert commands rapidly without await
            const promises = Array.from({ length: 10 }, (_, i) =>
                wsClient.command(
                    `from [{ id: ${i}, value: ${i * 10} }] insert test.${tableName}`,
                    null,
                    []
                )
            );

            await Promise.all(promises);

            // Wait for all callbacks
            await new Promise(resolve => setTimeout(resolve, 1000));

            // Rapid inserts are batched by the server into a single callback
            expect(tracker.getCallCount()).toBe(1);
            expect(tracker.getAllRows().length).toBe(10);

            // Verify all rows have correct values
            const rows = tracker.getAllRows();
            for (let i = 0; i < 10; i++) {
                const row = rows.find(r => r.id === i);
                expect(row).toBeDefined();
                expect(row?.value).toBe(i * 10);
            }

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);
    });
});
