// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Shape } from '@reifydb/core';
import { wait_for_database } from '../setup';
import {
    create_test_table_name,
    create_test_table,
    create_callback_tracker
} from './subscription-helpers';

describe('WebSocket Batch Subscriptions', () => {
    let ws_client: WsClient;
    const testUrl = process.env.REIFYDB_WS_URL || 'ws://localhost:18090';

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    beforeEach(async () => {
        ws_client = await Client.connect_ws(testUrl, {
            timeout_ms: 10000,
            token: process.env.REIFYDB_TOKEN,
            reconnect_delay_ms: 100
        });
    }, 15000);

    afterEach(async () => {
        if (ws_client) {
            ws_client.disconnect();
        }
    });

    describe('Basic Batch Flow', () => {
        it('should successfully batch_subscribe to multiple queries', async () => {
            const table_a = create_test_table_name('batch_a');
            const table_b = create_test_table_name('batch_b');
            await create_test_table(ws_client, table_a, ['id Int4', 'name Utf8']);
            await create_test_table(ws_client, table_b, ['id Int4', 'value Int4']);

            const tracker_a = create_callback_tracker();
            const tracker_b = create_callback_tracker();

            const batch = await ws_client.batch_subscribe([
                {
                    rql: `from test::${table_a}`,
                    shape: Shape.object({ id: Shape.number(), name: Shape.string() }),
                    callbacks: { on_insert: tracker_a.callback }
                },
                {
                    rql: `from test::${table_b}`,
                    shape: Shape.object({ id: Shape.number(), value: Shape.number() }),
                    callbacks: { on_insert: tracker_b.callback }
                }
            ]);

            expect(batch.batch_id).toBeDefined();
            expect(typeof batch.batch_id).toBe('string');
            expect(batch.batch_id.length).toBeGreaterThan(0);
            expect(batch.subscription_ids).toHaveLength(2);
            expect(batch.subscription_ids[0]).not.toBe(batch.subscription_ids[1]);

            const batches = (ws_client as any).batches;
            expect(batches.has(batch.batch_id)).toBe(true);

            await ws_client.batch_unsubscribe(batch.batch_id);
        }, 10000);

        it('should successfully batch_unsubscribe and clear state', async () => {
            const table_name = create_test_table_name('batch_unsub');
            await create_test_table(ws_client, table_name, ['id Int4']);

            const tracker = create_callback_tracker();
            const batch = await ws_client.batch_subscribe([
                {
                    rql: `from test::${table_name}`,
                    shape: Shape.object({ id: Shape.number() }),
                    callbacks: { on_insert: tracker.callback }
                }
            ]);

            await ws_client.batch_unsubscribe(batch.batch_id);

            const batches = (ws_client as any).batches;
            const sub_to_batch = (ws_client as any).sub_to_batch;
            expect(batches.has(batch.batch_id)).toBe(false);
            expect(sub_to_batch.has(batch.subscription_ids[0])).toBe(false);
        }, 10000);

        it('should reject batch_subscribe with empty members', async () => {
            await expect(ws_client.batch_subscribe([])).rejects.toThrow(
                /at least one member/i
            );
        });
    });

    describe('Per-member Routing', () => {
        it('should route INSERTs to the correct member callback', async () => {
            const table_a = create_test_table_name('batch_route_a');
            const table_b = create_test_table_name('batch_route_b');
            await create_test_table(ws_client, table_a, ['id Int4', 'name Utf8']);
            await create_test_table(ws_client, table_b, ['id Int4', 'value Int4']);

            const shape_a = Shape.object({ id: Shape.number(), name: Shape.string() });
            const shape_b = Shape.object({ id: Shape.number(), value: Shape.number() });

            const tracker_a = create_callback_tracker(shape_a);
            const tracker_b = create_callback_tracker(shape_b);

            const batch = await ws_client.batch_subscribe([
                { rql: `from test::${table_a}`, shape: shape_a, callbacks: { on_insert: tracker_a.callback } },
                { rql: `from test::${table_b}`, shape: shape_b, callbacks: { on_insert: tracker_b.callback } }
            ]);

            await ws_client.command(
                `INSERT test::${table_a} [{ id: 1, name: 'alice' }]`,
                null,
                []
            );
            await tracker_a.wait_for_call();

            expect(tracker_a.get_call_count()).toBe(1);
            expect(tracker_b.get_call_count()).toBe(0);
            const row_a = tracker_a.get_all_rows()[0];
            expect(row_a.id).toBe(1);
            expect(row_a.name).toBe('alice');

            await ws_client.command(
                `INSERT test::${table_b} [{ id: 42, value: 100 }]`,
                null,
                []
            );
            await tracker_b.wait_for_call();

            expect(tracker_a.get_call_count()).toBe(1);
            expect(tracker_b.get_call_count()).toBe(1);
            const row_b = tracker_b.get_all_rows()[0];
            expect(row_b.id).toBe(42);
            expect(row_b.value).toBe(100);

            await ws_client.batch_unsubscribe(batch.batch_id);
        }, 10000);

        it('should dispatch UPDATE and REMOVE to the right members', async () => {
            const table_a = create_test_table_name('batch_upd_a');
            const table_b = create_test_table_name('batch_upd_b');
            await create_test_table(ws_client, table_a, ['id Int4', 'name Utf8']);
            await create_test_table(ws_client, table_b, ['id Int4', 'name Utf8']);

            const shape = Shape.object({ id: Shape.number(), name: Shape.string() });

            const insert_a = create_callback_tracker(shape);
            const update_a = create_callback_tracker(shape);
            const remove_a = create_callback_tracker(shape);
            const insert_b = create_callback_tracker(shape);
            const update_b = create_callback_tracker(shape);
            const remove_b = create_callback_tracker(shape);

            const batch = await ws_client.batch_subscribe([
                {
                    rql: `from test::${table_a}`,
                    shape,
                    callbacks: {
                        on_insert: insert_a.callback,
                        on_update: update_a.callback,
                        on_remove: remove_a.callback
                    }
                },
                {
                    rql: `from test::${table_b}`,
                    shape,
                    callbacks: {
                        on_insert: insert_b.callback,
                        on_update: update_b.callback,
                        on_remove: remove_b.callback
                    }
                }
            ]);

            await ws_client.command(
                `INSERT test::${table_a} [{ id: 1, name: 'a' }]`,
                null,
                []
            );
            await insert_a.wait_for_call();

            await ws_client.command(
                `UPDATE test::${table_a} { name: 'a_upd' } FILTER id == 1`,
                null,
                []
            );
            await update_a.wait_for_call();

            expect(update_a.get_call_count()).toBe(1);
            expect(update_a.get_all_rows()[0].name).toBe('a_upd');
            expect(update_b.get_call_count()).toBe(0);

            await ws_client.command(
                `INSERT test::${table_b} [{ id: 2, name: 'b' }]`,
                null,
                []
            );
            await insert_b.wait_for_call();

            await ws_client.command(
                `DELETE test::${table_b} FILTER id == 2`,
                null,
                []
            );
            await remove_b.wait_for_call();

            expect(remove_b.get_call_count()).toBe(1);
            expect(remove_a.get_call_count()).toBe(0);

            await ws_client.batch_unsubscribe(batch.batch_id);
        }, 15000);

        it('should coalesce simultaneous writes across members', async () => {
            const table_a = create_test_table_name('batch_coal_a');
            const table_b = create_test_table_name('batch_coal_b');
            await create_test_table(ws_client, table_a, ['id Int4']);
            await create_test_table(ws_client, table_b, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker_a = create_callback_tracker(shape);
            const tracker_b = create_callback_tracker(shape);

            const batch = await ws_client.batch_subscribe([
                { rql: `from test::${table_a}`, shape, callbacks: { on_insert: tracker_a.callback } },
                { rql: `from test::${table_b}`, shape, callbacks: { on_insert: tracker_b.callback } }
            ]);

            await Promise.all([
                ws_client.command(`INSERT test::${table_a} [{ id: 1 }]`, null, []),
                ws_client.command(`INSERT test::${table_b} [{ id: 2 }]`, null, [])
            ]);

            await tracker_a.wait_for_rows(1);
            await tracker_b.wait_for_rows(1);

            expect(tracker_a.get_all_rows()[0].id).toBe(1);
            expect(tracker_b.get_all_rows()[0].id).toBe(2);

            await ws_client.batch_unsubscribe(batch.batch_id);
        }, 15000);

        it('should stop invoking callbacks after batch_unsubscribe', async () => {
            const table_name = create_test_table_name('batch_silence');
            await create_test_table(ws_client, table_name, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker = create_callback_tracker(shape);

            const batch = await ws_client.batch_subscribe([
                { rql: `from test::${table_name}`, shape, callbacks: { on_insert: tracker.callback } }
            ]);

            await ws_client.command(
                `INSERT test::${table_name} [{ id: 1 }]`,
                null,
                []
            );
            await tracker.wait_for_call();
            expect(tracker.get_call_count()).toBe(1);

            await ws_client.batch_unsubscribe(batch.batch_id);

            await ws_client.command(
                `INSERT test::${table_name} [{ id: 2 }]`,
                null,
                []
            );
            await new Promise(resolve => setTimeout(resolve, 300));

            expect(tracker.get_call_count()).toBe(1);
        }, 15000);
    });

    describe('Concurrent Subscriptions', () => {
        it('should keep two concurrent batches and a single subscription isolated', async () => {
            const table_x = create_test_table_name('conc_x');
            const table_y = create_test_table_name('conc_y');
            const table_z = create_test_table_name('conc_z');
            const table_w = create_test_table_name('conc_w');
            const table_s = create_test_table_name('conc_single');
            await create_test_table(ws_client, table_x, ['id Int4']);
            await create_test_table(ws_client, table_y, ['id Int4']);
            await create_test_table(ws_client, table_z, ['id Int4']);
            await create_test_table(ws_client, table_w, ['id Int4']);
            await create_test_table(ws_client, table_s, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker_x = create_callback_tracker(shape);
            const tracker_y = create_callback_tracker(shape);
            const tracker_z = create_callback_tracker(shape);
            const tracker_w = create_callback_tracker(shape);
            const tracker_s = create_callback_tracker(shape);

            const [batch_1, batch_2, single_id] = await Promise.all([
                ws_client.batch_subscribe([
                    { rql: `from test::${table_x}`, shape, callbacks: { on_insert: tracker_x.callback } },
                    { rql: `from test::${table_y}`, shape, callbacks: { on_insert: tracker_y.callback } }
                ]),
                ws_client.batch_subscribe([
                    { rql: `from test::${table_z}`, shape, callbacks: { on_insert: tracker_z.callback } },
                    { rql: `from test::${table_w}`, shape, callbacks: { on_insert: tracker_w.callback } }
                ]),
                ws_client.subscribe(
                    `from test::${table_s}`,
                    null,
                    shape,
                    { on_insert: tracker_s.callback }
                )
            ]);

            expect(batch_1.batch_id).not.toBe(batch_2.batch_id);
            expect(batch_1.subscription_ids).toHaveLength(2);
            expect(batch_2.subscription_ids).toHaveLength(2);
            const all_sub_ids = new Set([
                ...batch_1.subscription_ids,
                ...batch_2.subscription_ids,
                single_id
            ]);
            expect(all_sub_ids.size).toBe(5);

            const batches = (ws_client as any).batches;
            const sub_to_batch = (ws_client as any).sub_to_batch;
            const subscriptions = (ws_client as any).subscriptions;
            expect(batches.size).toBe(2);
            expect(sub_to_batch.size).toBe(4);
            expect(subscriptions.size).toBe(1);

            await Promise.all([
                ws_client.command(`INSERT test::${table_x} [{ id: 1 }]`, null, []),
                ws_client.command(`INSERT test::${table_y} [{ id: 2 }]`, null, []),
                ws_client.command(`INSERT test::${table_z} [{ id: 3 }]`, null, []),
                ws_client.command(`INSERT test::${table_w} [{ id: 4 }]`, null, []),
                ws_client.command(`INSERT test::${table_s} [{ id: 5 }]`, null, [])
            ]);

            await Promise.all([
                tracker_x.wait_for_rows(1),
                tracker_y.wait_for_rows(1),
                tracker_z.wait_for_rows(1),
                tracker_w.wait_for_rows(1),
                tracker_s.wait_for_rows(1)
            ]);

            expect(tracker_x.get_all_rows()[0].id).toBe(1);
            expect(tracker_y.get_all_rows()[0].id).toBe(2);
            expect(tracker_z.get_all_rows()[0].id).toBe(3);
            expect(tracker_w.get_all_rows()[0].id).toBe(4);
            expect(tracker_s.get_all_rows()[0].id).toBe(5);

            // Unsubscribe one batch; the other batch and the single sub must keep firing.
            await ws_client.batch_unsubscribe(batch_1.batch_id);
            expect(batches.size).toBe(1);
            expect(batches.has(batch_2.batch_id)).toBe(true);
            expect(sub_to_batch.size).toBe(2);
            expect(subscriptions.size).toBe(1);

            tracker_x.clear();
            tracker_y.clear();
            tracker_z.clear();
            tracker_w.clear();
            tracker_s.clear();

            await Promise.all([
                ws_client.command(`INSERT test::${table_x} [{ id: 11 }]`, null, []),
                ws_client.command(`INSERT test::${table_z} [{ id: 13 }]`, null, []),
                ws_client.command(`INSERT test::${table_s} [{ id: 15 }]`, null, [])
            ]);
            await Promise.all([
                tracker_z.wait_for_rows(1),
                tracker_s.wait_for_rows(1)
            ]);
            await new Promise(resolve => setTimeout(resolve, 200));

            expect(tracker_x.get_call_count()).toBe(0);
            expect(tracker_z.get_all_rows()[0].id).toBe(13);
            expect(tracker_s.get_all_rows()[0].id).toBe(15);

            await ws_client.batch_unsubscribe(batch_2.batch_id);
            await ws_client.unsubscribe(single_id);
        }, 20000);
    });

    describe('RBCF Transport', () => {
        it('should route batch changes over RBCF binary format', async () => {
            if (ws_client) ws_client.disconnect();
            ws_client = await Client.connect_ws(testUrl, {
                timeout_ms: 10000,
                token: process.env.REIFYDB_TOKEN,
                reconnect_delay_ms: 100,
                format: 'rbcf'
            });

            const table_a = create_test_table_name('batch_rbcf_a');
            const table_b = create_test_table_name('batch_rbcf_b');
            await create_test_table(ws_client, table_a, ['id Int4']);
            await create_test_table(ws_client, table_b, ['id Int4']);

            const shape = Shape.object({ id: Shape.number() });
            const tracker_a = create_callback_tracker(shape);
            const tracker_b = create_callback_tracker(shape);

            const batch = await ws_client.batch_subscribe([
                { rql: `from test::${table_a}`, shape, callbacks: { on_insert: tracker_a.callback } },
                { rql: `from test::${table_b}`, shape, callbacks: { on_insert: tracker_b.callback } }
            ]);

            await ws_client.command(
                `INSERT test::${table_a} [{ id: 10 }]`,
                null,
                []
            );
            await tracker_a.wait_for_call();

            await ws_client.command(
                `INSERT test::${table_b} [{ id: 20 }]`,
                null,
                []
            );
            await tracker_b.wait_for_call();

            expect(tracker_a.get_all_rows()[0].id).toBe(10);
            expect(tracker_b.get_all_rows()[0].id).toBe(20);

            await ws_client.batch_unsubscribe(batch.batch_id);
        }, 15000);
    });
});
