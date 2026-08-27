// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { DurationValue, Shape } from '@reifydb/core';
import { waitForDatabase } from '../setup';
import {
    createTestTableName,
    createTestTable,
    createCallbackTracker
} from './subscription-helpers';

describe('WebSocket Batch Subscriptions', () => {
    let wsClient: WsClient;
    const testUrl = process.env.REIFYDB_WS_URL || 'ws://localhost:18090';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        wsClient = await Client.connectWs(testUrl, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN,
            reconnectDelayMs: 100
        });
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            wsClient.disconnect();
        }
    });

    describe('Basic Batch Flow', () => {
        it('should successfully batchSubscribe to multiple queries', async () => {
            const tableA = createTestTableName('batch_a');
            const tableB = createTestTableName('batch_b');
            await createTestTable(wsClient, tableA, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, tableB, ['id Int4', 'value Int4']);

            const trackerA = createCallbackTracker();
            const trackerB = createCallbackTracker();

            const batch = await wsClient.batchSubscribe([
                {
                    rql: `from test::${tableA}`,
                    shape: Shape.object({ id: Shape.number(), name: Shape.string() }),
                    callbacks: { onInsert: trackerA.callback }
                },
                {
                    rql: `from test::${tableB}`,
                    shape: Shape.object({ id: Shape.number(), value: Shape.number() }),
                    callbacks: { onInsert: trackerB.callback }
                }
            ]);

            expect(batch.batchId).toBeDefined();
            expect(typeof batch.batchId).toBe('string');
            expect(batch.batchId.length).toBeGreaterThan(0);
            expect(batch.subscriptionIds).toHaveLength(2);
            expect(batch.subscriptionIds[0]).not.toBe(batch.subscriptionIds[1]);

            const batches = (wsClient as any).batches;
            expect(batches.has(batch.batchId)).toBe(true);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 10000);

        it('should successfully batchUnsubscribe and clear state', async () => {
            const tableName = createTestTableName('batch_unsub');
            await createTestTable(wsClient, tableName, ['id Int4']);

            const tracker = createCallbackTracker();
            const batch = await wsClient.batchSubscribe([
                {
                    rql: `from test::${tableName}`,
                    shape: Shape.object({ id: Shape.number() }),
                    callbacks: { onInsert: tracker.callback }
                }
            ]);

            await wsClient.batchUnsubscribe(batch.batchId);

            const batches = (wsClient as any).batches;
            const subToBatch = (wsClient as any).subToBatch;
            expect(batches.has(batch.batchId)).toBe(false);
            expect(subToBatch.has(batch.subscriptionIds[0])).toBe(false);
        }, 10000);

        it('should reject batchSubscribe with empty members', async () => {
            await expect(wsClient.batchSubscribe([])).rejects.toThrow(
                /at least one member/i
            );
        });
    });

    describe('Per-member Routing', () => {
        it('should route INSERTs to the correct member callback', async () => {
            const tableA = createTestTableName('batch_route_a');
            const tableB = createTestTableName('batch_route_b');
            await createTestTable(wsClient, tableA, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, tableB, ['id Int4', 'value Int4']);

            const shapeA = Shape.object({ id: Shape.number(), name: Shape.string() });
            const shapeB = Shape.object({ id: Shape.number(), value: Shape.number() });

            const trackerA = createCallbackTracker(shapeA);
            const trackerB = createCallbackTracker(shapeB);

            const batch = await wsClient.batchSubscribe([
                { rql: `from test::${tableA}`, shape: shapeA, callbacks: { onInsert: trackerA.callback } },
                { rql: `from test::${tableB}`, shape: shapeB, callbacks: { onInsert: trackerB.callback } }
            ]);

            await wsClient.command(
                `INSERT test::${tableA} [{ id: 1, name: 'alice' }]`,
                null,
                []
            );
            await trackerA.waitForCall();

            expect(trackerA.getCallCount()).toBe(1);
            expect(trackerB.getCallCount()).toBe(0);
            const rowA = trackerA.getAllRows()[0];
            expect(rowA.id).toBe(1);
            expect(rowA.name).toBe('alice');

            await wsClient.command(
                `INSERT test::${tableB} [{ id: 42, value: 100 }]`,
                null,
                []
            );
            await trackerB.waitForCall();

            expect(trackerA.getCallCount()).toBe(1);
            expect(trackerB.getCallCount()).toBe(1);
            const rowB = trackerB.getAllRows()[0];
            expect(rowB.id).toBe(42);
            expect(rowB.value).toBe(100);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 10000);

        it('should dispatch UPDATE and REMOVE to the right members', async () => {
            const tableA = createTestTableName('batch_upd_a');
            const tableB = createTestTableName('batch_upd_b');
            await createTestTable(wsClient, tableA, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, tableB, ['id Int4', 'name Utf8']);

            const shape = Shape.object({ id: Shape.number(), name: Shape.string() });

            const insertA = createCallbackTracker(shape);
            const updateA = createCallbackTracker(shape);
            const removeA = createCallbackTracker(shape);
            const insertB = createCallbackTracker(shape);
            const updateB = createCallbackTracker(shape);
            const removeB = createCallbackTracker(shape);

            const batch = await wsClient.batchSubscribe([
                {
                    rql: `from test::${tableA}`,
                    shape,
                    callbacks: {
                        onInsert: insertA.callback,
                        onUpdate: updateA.callback,
                        onRemove: removeA.callback
                    }
                },
                {
                    rql: `from test::${tableB}`,
                    shape,
                    callbacks: {
                        onInsert: insertB.callback,
                        onUpdate: updateB.callback,
                        onRemove: removeB.callback
                    }
                }
            ]);

            await wsClient.command(
                `INSERT test::${tableA} [{ id: 1, name: 'a' }]`,
                null,
                []
            );
            await insertA.waitForCall();

            await wsClient.command(
                `UPDATE test::${tableA} { name: 'a_upd' } FILTER id == 1`,
                null,
                []
            );
            await updateA.waitForCall();

            expect(updateA.getCallCount()).toBe(1);
            expect(updateA.getAllRows()[0].name).toBe('a_upd');
            expect(updateB.getCallCount()).toBe(0);

            await wsClient.command(
                `INSERT test::${tableB} [{ id: 2, name: 'b' }]`,
                null,
                []
            );
            await insertB.waitForCall();

            await wsClient.command(
                `DELETE test::${tableB} FILTER id == 2`,
                null,
                []
            );
            await removeB.waitForCall();

            expect(removeB.getCallCount()).toBe(1);
            expect(removeA.getCallCount()).toBe(0);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 15000);

        it('should coalesce simultaneous writes across members', async () => {
            const tableA = createTestTableName('batch_coal_a');
            const tableB = createTestTableName('batch_coal_b');
            await createTestTable(wsClient, tableA, ['id Int4']);
            await createTestTable(wsClient, tableB, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const trackerA = createCallbackTracker(shape);
            const trackerB = createCallbackTracker(shape);

            const batch = await wsClient.batchSubscribe([
                { rql: `from test::${tableA}`, shape, callbacks: { onInsert: trackerA.callback } },
                { rql: `from test::${tableB}`, shape, callbacks: { onInsert: trackerB.callback } }
            ]);

            await Promise.all([
                wsClient.command(`INSERT test::${tableA} [{ id: 1 }]`, null, []),
                wsClient.command(`INSERT test::${tableB} [{ id: 2 }]`, null, [])
            ]);

            await trackerA.waitForRows(1);
            await trackerB.waitForRows(1);

            expect(trackerA.getAllRows()[0].id).toBe(1);
            expect(trackerB.getAllRows()[0].id).toBe(2);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 15000);

        it('should stop invoking callbacks after batchUnsubscribe', async () => {
            const tableName = createTestTableName('batch_silence');
            await createTestTable(wsClient, tableName, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker = createCallbackTracker(shape);

            const batch = await wsClient.batchSubscribe([
                { rql: `from test::${tableName}`, shape, callbacks: { onInsert: tracker.callback } }
            ]);

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 1 }]`,
                null,
                []
            );
            await tracker.waitForCall();
            expect(tracker.getCallCount()).toBe(1);

            await wsClient.batchUnsubscribe(batch.batchId);

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 2 }]`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            expect(tracker.getCallCount()).toBe(1);
        }, 15000);
    });

    describe('Concurrent Subscriptions', () => {
        it('should keep two concurrent batches and a single subscription isolated', async () => {
            const tableX = createTestTableName('conc_x');
            const tableY = createTestTableName('conc_y');
            const tableZ = createTestTableName('conc_z');
            const tableW = createTestTableName('conc_w');
            const tableS = createTestTableName('conc_single');
            await createTestTable(wsClient, tableX, ['id Int4']);
            await createTestTable(wsClient, tableY, ['id Int4']);
            await createTestTable(wsClient, tableZ, ['id Int4']);
            await createTestTable(wsClient, tableW, ['id Int4']);
            await createTestTable(wsClient, tableS, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const trackerX = createCallbackTracker(shape);
            const trackerY = createCallbackTracker(shape);
            const trackerZ = createCallbackTracker(shape);
            const trackerW = createCallbackTracker(shape);
            const trackerS = createCallbackTracker(shape);

            const [batch1, batch2, singleId] = await Promise.all([
                wsClient.batchSubscribe([
                    { rql: `from test::${tableX}`, shape, callbacks: { onInsert: trackerX.callback } },
                    { rql: `from test::${tableY}`, shape, callbacks: { onInsert: trackerY.callback } }
                ]),
                wsClient.batchSubscribe([
                    { rql: `from test::${tableZ}`, shape, callbacks: { onInsert: trackerZ.callback } },
                    { rql: `from test::${tableW}`, shape, callbacks: { onInsert: trackerW.callback } }
                ]),
                wsClient.subscribe(
                    `from test::${tableS}`,
                    null,
                    shape,
                    { onInsert: trackerS.callback }
                )
            ]);

            expect(batch1.batchId).not.toBe(batch2.batchId);
            expect(batch1.subscriptionIds).toHaveLength(2);
            expect(batch2.subscriptionIds).toHaveLength(2);
            const allSubIds = new Set([
                ...batch1.subscriptionIds,
                ...batch2.subscriptionIds,
                singleId
            ]);
            expect(allSubIds.size).toBe(5);

            const batches = (wsClient as any).batches;
            const subToBatch = (wsClient as any).subToBatch;
            const subscriptions = (wsClient as any).subscriptions;
            expect(batches.size).toBe(2);
            expect(subToBatch.size).toBe(4);
            expect(subscriptions.size).toBe(1);

            await Promise.all([
                wsClient.command(`INSERT test::${tableX} [{ id: 1 }]`, null, []),
                wsClient.command(`INSERT test::${tableY} [{ id: 2 }]`, null, []),
                wsClient.command(`INSERT test::${tableZ} [{ id: 3 }]`, null, []),
                wsClient.command(`INSERT test::${tableW} [{ id: 4 }]`, null, []),
                wsClient.command(`INSERT test::${tableS} [{ id: 5 }]`, null, [])
            ]);

            await Promise.all([
                trackerX.waitForRows(1),
                trackerY.waitForRows(1),
                trackerZ.waitForRows(1),
                trackerW.waitForRows(1),
                trackerS.waitForRows(1)
            ]);

            expect(trackerX.getAllRows()[0].id).toBe(1);
            expect(trackerY.getAllRows()[0].id).toBe(2);
            expect(trackerZ.getAllRows()[0].id).toBe(3);
            expect(trackerW.getAllRows()[0].id).toBe(4);
            expect(trackerS.getAllRows()[0].id).toBe(5);

            // Unsubscribe one batch; the other batch and the single sub must keep firing.
            await wsClient.batchUnsubscribe(batch1.batchId);
            expect(batches.size).toBe(1);
            expect(batches.has(batch2.batchId)).toBe(true);
            expect(subToBatch.size).toBe(2);
            expect(subscriptions.size).toBe(1);

            trackerX.clear();
            trackerY.clear();
            trackerZ.clear();
            trackerW.clear();
            trackerS.clear();

            await Promise.all([
                wsClient.command(`INSERT test::${tableX} [{ id: 11 }]`, null, []),
                wsClient.command(`INSERT test::${tableZ} [{ id: 13 }]`, null, []),
                wsClient.command(`INSERT test::${tableS} [{ id: 15 }]`, null, [])
            ]);
            await Promise.all([
                trackerZ.waitForRows(1),
                trackerS.waitForRows(1)
            ]);
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(trackerX.getCallCount()).toBe(0);
            expect(trackerZ.getAllRows()[0].id).toBe(13);
            expect(trackerS.getAllRows()[0].id).toBe(15);

            await wsClient.batchUnsubscribe(batch2.batchId);
            await wsClient.unsubscribe(singleId);
        }, 20000);
    });

    describe('RBCF Transport', () => {
        it('should route batch changes over RBCF binary format', async () => {
            if (wsClient) wsClient.disconnect();
            wsClient = await Client.connectWs(testUrl, {
                timeoutMs: 10000,
                token: process.env.REIFYDB_TOKEN,
                reconnectDelayMs: 100,
                format: 'rbcf'
            });

            const tableA = createTestTableName('batch_rbcf_a');
            const tableB = createTestTableName('batch_rbcf_b');
            await createTestTable(wsClient, tableA, ['id Int4']);
            await createTestTable(wsClient, tableB, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const trackerA = createCallbackTracker(shape);
            const trackerB = createCallbackTracker(shape);

            const batch = await wsClient.batchSubscribe([
                { rql: `from test::${tableA}`, shape, callbacks: { onInsert: trackerA.callback } },
                { rql: `from test::${tableB}`, shape, callbacks: { onInsert: trackerB.callback } }
            ]);

            await wsClient.command(
                `INSERT test::${tableA} [{ id: 10 }]`,
                null,
                []
            );
            await trackerA.waitForCall();

            await wsClient.command(
                `INSERT test::${tableB} [{ id: 20 }]`,
                null,
                []
            );
            await trackerB.waitForCall();

            expect(trackerA.getAllRows()[0].id).toBe(10);
            expect(trackerB.getAllRows()[0].id).toBe(20);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 15000);
    });

    describe('Linger Coalescing (regression)', () => {
        // A linger window coalesces several change-events into a single push whose
        // body carries one frame per event. handleChangeMessage previously read
        // only frames[0] and dropped the rest, so a linger'd subscription silently
        // lost every row except the first of each coalesced push (observed as 16
        // live inserts collapsing to 2). Without linger each change is its own
        // single-frame push, which hid the bug. The fix dispatches every frame.
        it('delivers every row when linger coalesces multiple changes into one push', async () => {
            const table = createTestTableName('linger_coalesce');
            await createTestTable(wsClient, table, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker = createCallbackTracker(shape);

            const N = 8;
            const LINGER_MS = 500;

            const batch = await wsClient.batchSubscribe([
                {
                    rql: `from test::${table}`,
                    shape,
                    callbacks: { onInsert: tracker.callback },
                    config: { linger: DurationValue.fromMilliseconds(LINGER_MS) }
                }
            ]);

            // N separate inserts (N transactions => N frames) fired inside one
            // linger window, so the server coalesces them into a single push whose
            // body carries all N frames.
            for (let i = 1; i <= N; i++) {
                await wsClient.command(`INSERT test::${table} [{ id: ${i} }]`, null, []);
            }

            // The pre-fix client surfaced only the first frame per push, so this
            // would never reach N and would time out; the fix must surface all N.
            await tracker.waitForRows(N, LINGER_MS + 4000);

            const ids = tracker.getAllRows().map(r => r.id).sort((a, b) => a - b);
            expect(ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);

            await wsClient.batchUnsubscribe(batch.batchId);
        }, 15000);
    });
});
