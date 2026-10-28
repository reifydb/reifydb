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

describe('WebSocket Call Subscriptions', () => {
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

    describe('Basic Call Flow', () => {
        it('should subscribe to multiple queries in one call', async () => {
            const tableA = createTestTableName('batch_a');
            const tableB = createTestTableName('batch_b');
            await createTestTable(wsClient, tableA, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, tableB, ['id Int4', 'value Int4']);

            const trackerA = createCallbackTracker();
            const trackerB = createCallbackTracker();

            const ids = await wsClient.subscribe([
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

            expect(ids).toHaveLength(2);
            expect(typeof ids[0]).toBe('string');
            expect(ids[0].length).toBeGreaterThan(0);
            expect(typeof ids[1]).toBe('string');
            expect(ids[1].length).toBeGreaterThan(0);
            expect(ids[0]).not.toBe(ids[1]);

            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.has(ids[0])).toBe(true);
            expect(subscriptions.has(ids[1])).toBe(true);

            await wsClient.unsubscribe(ids);
        }, 10000);

        it('should unsubscribe a list and clear state', async () => {
            const tableName = createTestTableName('batch_unsub');
            await createTestTable(wsClient, tableName, ['id Int4']);

            const tracker = createCallbackTracker();
            const ids = await wsClient.subscribe([
                {
                    rql: `from test::${tableName}`,
                    shape: Shape.object({ id: Shape.number() }),
                    callbacks: { onInsert: tracker.callback }
                }
            ]);

            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.has(ids[0])).toBe(true);

            await wsClient.unsubscribe(ids);

            expect(subscriptions.has(ids[0])).toBe(false);
        }, 10000);

        it('should reject subscribe with empty subscriptions', async () => {
            await expect(wsClient.subscribe([])).rejects.toThrow(
                /at least one subscription/i
            );
        });
    });

    describe('Per-subscription Routing', () => {
        it('should route INSERTs to the correct subscription callback', async () => {
            const tableA = createTestTableName('batch_route_a');
            const tableB = createTestTableName('batch_route_b');
            await createTestTable(wsClient, tableA, ['id Int4', 'name Utf8']);
            await createTestTable(wsClient, tableB, ['id Int4', 'value Int4']);

            const shapeA = Shape.object({ id: Shape.number(), name: Shape.string() });
            const shapeB = Shape.object({ id: Shape.number(), value: Shape.number() });

            const trackerA = createCallbackTracker(shapeA);
            const trackerB = createCallbackTracker(shapeB);

            const ids = await wsClient.subscribe([
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

            await wsClient.unsubscribe(ids);
        }, 10000);

        it('should dispatch UPDATE and REMOVE to the right subscriptions', async () => {
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

            const ids = await wsClient.subscribe([
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

            await wsClient.unsubscribe(ids);
        }, 15000);

        it('should coalesce simultaneous writes across subscriptions', async () => {
            const tableA = createTestTableName('batch_coal_a');
            const tableB = createTestTableName('batch_coal_b');
            await createTestTable(wsClient, tableA, ['id Int4']);
            await createTestTable(wsClient, tableB, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const trackerA = createCallbackTracker(shape);
            const trackerB = createCallbackTracker(shape);

            const ids = await wsClient.subscribe([
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

            await wsClient.unsubscribe(ids);
        }, 15000);

        it('should stop invoking callbacks after unsubscribe', async () => {
            const tableName = createTestTableName('batch_silence');
            await createTestTable(wsClient, tableName, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker = createCallbackTracker(shape);

            const ids = await wsClient.subscribe([
                { rql: `from test::${tableName}`, shape, callbacks: { onInsert: tracker.callback } }
            ]);

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 1 }]`,
                null,
                []
            );
            await tracker.waitForCall();
            expect(tracker.getCallCount()).toBe(1);

            await wsClient.unsubscribe(ids);

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
        it('should keep two concurrent calls and a single subscription isolated', async () => {
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

            const [ids1, ids2, [singleId]] = await Promise.all([
                wsClient.subscribe([
                    { rql: `from test::${tableX}`, shape, callbacks: { onInsert: trackerX.callback } },
                    { rql: `from test::${tableY}`, shape, callbacks: { onInsert: trackerY.callback } }
                ]),
                wsClient.subscribe([
                    { rql: `from test::${tableZ}`, shape, callbacks: { onInsert: trackerZ.callback } },
                    { rql: `from test::${tableW}`, shape, callbacks: { onInsert: trackerW.callback } }
                ]),
                wsClient.subscribe([
                    { rql: `from test::${tableS}`, params: null, shape, callbacks: { onInsert: trackerS.callback } }
                ])
            ]);

            expect(ids1).toHaveLength(2);
            expect(ids2).toHaveLength(2);
            const allSubIds = new Set([
                ...ids1,
                ...ids2,
                singleId
            ]);
            expect(allSubIds.size).toBe(5);

            const subscriptions = (wsClient as any).subscriptions;
            expect(subscriptions.size).toBe(5);

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

            // Unsubscribe one list; the other list and the single sub must keep firing.
            await wsClient.unsubscribe(ids1);
            expect(subscriptions.size).toBe(3);
            expect(subscriptions.has(ids1[0])).toBe(false);
            expect(subscriptions.has(ids1[1])).toBe(false);
            expect(subscriptions.has(ids2[0])).toBe(true);
            expect(subscriptions.has(ids2[1])).toBe(true);
            expect(subscriptions.has(singleId)).toBe(true);

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

            await wsClient.unsubscribe(ids2);
            await wsClient.unsubscribe([singleId]);
        }, 20000);
    });

    describe('RBCF Transport', () => {
        it('should route call changes over RBCF binary format', async () => {
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

            const ids = await wsClient.subscribe([
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

            await wsClient.unsubscribe(ids);
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

            const subscriptionIds = await wsClient.subscribe([
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

            await wsClient.unsubscribe(subscriptionIds);
        }, 15000);
    });
});
