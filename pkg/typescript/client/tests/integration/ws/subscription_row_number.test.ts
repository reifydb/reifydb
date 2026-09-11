// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Shape } from '@reifydb/core';
import { waitForDatabase } from '../setup';
import { CONTENT_TYPE_FRAMES } from '../../../src/content-types';
import {
    createTestTableName,
    createTestTable,
    createCallbackTracker
} from './subscription-helpers';

// `#rownum` is the server's identity for a subscription row. @reifydb/store keys every piece of
// client-side state on it, and the user column `id` cannot stand in: it may be absent, and nothing
// makes it unique. These tests pin the stamping itself, not the rows it rides on.
describe('WebSocket Subscription Row Numbers', () => {
    let wsClient: WsClient;
    const testUrl = process.env.REIFYDB_WS_URL || 'ws://localhost:18090';

    beforeAll(async () => {
        await waitForDatabase();
    }, 30000);

    beforeEach(async () => {
        wsClient = await Client.connectWs(testUrl, {
            timeoutMs: 10000,
            token: process.env.REIFYDB_TOKEN
        });
    }, 15000);

    afterEach(async () => {
        if (wsClient) {
            wsClient.disconnect();
        }
    });

    describe('Stamping from the frame', () => {
        it('should stamp each row of a frame with its own entry from row_numbers', async () => {
            const tableName = createTestTableName('rownum_stamp');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            // The frame is what the stamping reads, so the test has to see it too; wrapping the
            // change handler records the frames the real server sent without changing what runs.
            // The raw frames still carry row_numbers here, since reading the wire types only
            // rewrites columns.
            const client = wsClient as any;
            const dispatched: any[] = [];
            const handle = client.handleChangeMessage.bind(client);
            client.handleChangeMessage = (msg: any) => {
                dispatched.push(...(msg?.payload?.body?.frames ?? []));
                return handle(msg);
            };

            const tracker = createCallbackTracker(shape);
            const subscriptionId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                shape,
                {
                    onInsert: tracker.callback
                }
            );

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }, { id: 3, name: 'carol' }]`,
                null,
                []
            );
            await tracker.waitForRows(3);

            const rows = tracker.getAllRows();
            expect(rows.length).toBe(3);

            const rowNumbers = dispatched.flatMap(frame => Array.from(frame.row_numbers));
            expect(rowNumbers.length).toBe(3);

            rows.forEach((row, i) => {
                expect(row['#rownum']).toBe(Number(rowNumbers[i]));
            });

            // Each row carries its own identity; one number reused across the frame would make every
            // row in it collapse onto a single entry in a store keyed by `#rownum`.
            const distinct = new Set(rows.map(row => row['#rownum']));
            expect(distinct.size).toBe(3);

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);

        it('should hand onUpdate and onRemove the row number the insert already carried', async () => {
            const tableName = createTestTableName('rownum_ops');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const insertTracker = createCallbackTracker(shape);
            const updateTracker = createCallbackTracker(shape);
            const removeTracker = createCallbackTracker(shape);

            const subscriptionId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                shape,
                {
                    onInsert: insertTracker.callback,
                    onUpdate: updateTracker.callback,
                    onRemove: removeTracker.callback
                }
            );

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }]`,
                null,
                []
            );
            await insertTracker.waitForRows(2);

            const inserted = insertTracker.getAllRows();
            const aliceRowNumber = inserted.find(row => row.id === 1)!['#rownum'];
            const bobRowNumber = inserted.find(row => row.id === 2)!['#rownum'];
            expect(aliceRowNumber).toEqual(expect.any(Number));
            expect(bobRowNumber).toEqual(expect.any(Number));
            expect(aliceRowNumber).not.toBe(bobRowNumber);

            await wsClient.command(
                `UPDATE test::${tableName} { name: 'alice_updated' } FILTER id == 1`,
                null,
                []
            );
            await updateTracker.waitForRowMatching(row => row.name === 'alice_updated');

            await wsClient.command(
                `DELETE test::${tableName} FILTER id == 2`,
                null,
                []
            );
            await removeTracker.waitForRowMatching(row => row.id === 2);

            // The identity is the row's, not the delivery's: a store that inserted under one number
            // has to be able to update and remove under the same one.
            expect(updateTracker.getAllRows()[0]['#rownum']).toBe(aliceRowNumber);
            expect(removeTracker.getAllRows()[0]['#rownum']).toBe(bobRowNumber);

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);

        it('should stamp the row whether or not a shape reshapes it', async () => {
            const tableName = createTestTableName('rownum_shape');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const shapedTracker = createCallbackTracker(shape);
            const rawTracker = createCallbackTracker();

            const shapedId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                shape,
                {
                    onInsert: shapedTracker.callback
                }
            );
            const rawId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                undefined,
                {
                    onInsert: rawTracker.callback
                }
            );

            await wsClient.command(
                `INSERT test::${tableName} [{ id: 7, name: 'dora' }]`,
                null,
                []
            );
            await shapedTracker.waitForRows(1);
            await rawTracker.waitForRows(1);

            const shapedRow = shapedTracker.getAllRows()[0];
            const rawRow = rawTracker.getAllRows()[0];

            // The transform rebuilds the row into a fresh object under its own key names, so the two
            // subscriptions have to arrive at the same identity for the same underlying row.
            expect(shapedRow['#rownum']).toEqual(expect.any(Number));
            expect(rawRow['#rownum']).toEqual(expect.any(Number));
            expect(shapedRow['#rownum']).toBe(rawRow['#rownum']);

            // The shape decoded the columns to primitives; the stamp rode through that untouched.
            expect(shapedRow.id).toBe(7);
            expect(shapedRow.name).toBe('dora');

            await wsClient.unsubscribe(shapedId);
            await wsClient.unsubscribe(rawId);
        }, 15000);
    });

    // The frames below are fed to the change handler directly. A live server always sends
    // `row_numbers`, and always sends them as JSON numbers, so neither the missing-key case nor the
    // widening of the raw wire value can be induced through a subscription.
    describe('Frames the server does not currently send', () => {
        it('should read the row number as a number, not the raw wire value', async () => {
            const tableName = createTestTableName('rownum_number');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const tracker = createCallbackTracker(shape);
            const subscriptionId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                shape,
                {
                    onInsert: tracker.callback
                }
            );

            (wsClient as any).handleChangeMessage({
                type: 'Change',
                payload: {
                    subscriptionId,
                    contentType: CONTENT_TYPE_FRAMES,
                    body: {
                        frames: [{
                            op: 1,
                            row_numbers: ['7', '8'],
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['1', '2']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['alice', 'bob']}
                            ]
                        }]
                    }
                }
            });

            const rows = tracker.getAllRows();
            expect(rows.length).toBe(2);
            // A bigint or a string here would break every `===` against a number the caller holds.
            expect(rows[0]['#rownum']).toBe(7);
            expect(rows[1]['#rownum']).toBe(8);
            expect(typeof rows[0]['#rownum']).toBe('number');
            expect(typeof rows[1]['#rownum']).toBe('number');

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);

        it('should leave the key off a frame that carries no row numbers', async () => {
            const tableName = createTestTableName('rownum_absent');
            await createTestTable(wsClient, tableName, [
                'id Int4',
                'name Utf8'
            ]);

            const shape = Shape.object({
                id: Shape.number(),
                name: Shape.string()
            });

            const tracker = createCallbackTracker(shape);
            const subscriptionId = await wsClient.subscribe(
                `from test::${tableName}`,
                null,
                shape,
                {
                    onInsert: tracker.callback
                }
            );

            (wsClient as any).handleChangeMessage({
                type: 'Change',
                payload: {
                    subscriptionId,
                    contentType: CONTENT_TYPE_FRAMES,
                    body: {
                        frames: [{
                            op: 1,
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['1']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['alice']}
                            ]
                        }]
                    }
                }
            });

            const rows = tracker.getAllRows();
            expect(rows.length).toBe(1);
            // Absent, not undefined and not 0: a caller checking `'#rownum' in row` has to be able to
            // tell a row that has no identity from one whose identity happens to be falsy.
            expect('#rownum' in rows[0]).toBe(false);
            expect(rows[0].id).toBe(1);
            expect(rows[0].name).toBe('alice');

            await wsClient.unsubscribe(subscriptionId);
        }, 15000);
    });
});
