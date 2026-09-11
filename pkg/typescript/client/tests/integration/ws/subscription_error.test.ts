// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Shape } from '@reifydb/core';
import { waitForDatabase } from '../setup';
import { CONTENT_TYPE_FRAMES } from '../../../src/content-types';
import {
    createTestTableName,
    createTestTable,
    createCallbackTracker
} from './subscription-helpers';

/**
 * Waits for a condition to hold, polling until the timeout expires.
 */
async function waitUntil(predicate: () => boolean, timeoutMs: number = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 25));
    }
    throw new Error(`Condition never held within ${timeoutMs}ms`);
}

// A change that cannot be decoded has to reach the subscriber rather than vanish, and it must not
// take the rest of the message, or the rest of the connection, down with it.
describe('WebSocket Subscription Error Reporting', () => {
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

    it('should report an undecodable frame to onError instead of dropping it', async () => {
        const tableName = createTestTableName('sub_err_report');
        await createTestTable(wsClient, tableName, [
            'id Int4',
            'name Utf8'
        ]);

        // The shape names a column the table does not have, so every frame the server sends for this
        // subscription fails the shape check on the way in.
        const shape = Shape.object({
            id: Shape.number(),
            missing: Shape.string()
        });

        const errors: Error[] = [];
        const insertTracker = createCallbackTracker();

        const subscriptionId = await wsClient.subscribe(
            `from test::${tableName}`,
            null,
            shape,
            {
                onInsert: insertTracker.callback,
                onError: (error) => errors.push(error)
            }
        );

        await wsClient.command(
            `INSERT test::${tableName} [{ id: 1, name: 'alice' }]`,
            null,
            []
        );

        await waitUntil(() => errors.length > 0);

        expect(errors.length).toBe(1);
        expect(errors[0]).toBeInstanceOf(Error);
        expect(errors[0].message).toContain('missing');
        // The rows never decoded, so handing them to onInsert would hand the caller a lie.
        expect(insertTracker.getCallCount()).toBe(0);

        await wsClient.unsubscribe(subscriptionId);
    }, 15000);

    it('should deliver the frames that follow an undecodable one in the same message', async () => {
        const tableName = createTestTableName('sub_err_survive');
        await createTestTable(wsClient, tableName, [
            'id Int4',
            'name Utf8'
        ]);

        const shape = Shape.object({
            id: Shape.number(),
            name: Shape.string()
        });

        const errors: Error[] = [];
        const insertTracker = createCallbackTracker(shape);
        const updateTracker = createCallbackTracker(shape);

        const subscriptionId = await wsClient.subscribe(
            `from test::${tableName}`,
            null,
            shape,
            {
                onInsert: insertTracker.callback,
                onUpdate: updateTracker.callback,
                onError: (error) => errors.push(error)
            }
        );

        // A live server sends one frame per change message, so a message whose first frame fails
        // cannot be induced through a subscription; the handler is driven directly instead.
        (wsClient as any).handleChangeMessage({
            type: 'Change',
            payload: {
                subscriptionId,
                contentType: CONTENT_TYPE_FRAMES,
                body: {
                    frames: [
                        {
                            op: 1,
                            row_numbers: [1],
                            columns: [
                                {name: 'unexpected', type: {id: 'Int4'}, payload: ['1']}
                            ]
                        },
                        {
                            op: 1,
                            row_numbers: [2],
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['2']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['bob']}
                            ]
                        },
                        {
                            op: 2,
                            row_numbers: [3],
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['3']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['carol']}
                            ]
                        }
                    ]
                }
            }
        });

        expect(errors.length).toBe(1);
        expect(errors[0]).toBeInstanceOf(Error);

        // Both frames behind the failing one still reached their own callbacks, and each kept the
        // identity its frame carried.
        expect(insertTracker.getAllRows()).toEqual([
            {id: 2, name: 'bob', '#rownum': 2}
        ]);
        expect(updateTracker.getAllRows()).toEqual([
            {id: 3, name: 'carol', '#rownum': 3}
        ]);

        await wsClient.unsubscribe(subscriptionId);
    }, 15000);

    it('should report the failure without an onError callback and keep the connection usable', async () => {
        const consoleErrorSpy = vi.spyOn(console, 'error');

        const tableName = createTestTableName('sub_err_noop');
        await createTestTable(wsClient, tableName, [
            'id Int4',
            'name Utf8'
        ]);

        const shape = Shape.object({
            id: Shape.number(),
            missing: Shape.string()
        });

        const insertTracker = createCallbackTracker();

        // No onError: the failure has nowhere to go but the console, and must not escape into the
        // socket handler, where it would stall every later message on this connection.
        const subscriptionId = await wsClient.subscribe(
            `from test::${tableName}`,
            null,
            shape,
            {
                onInsert: insertTracker.callback
            }
        );

        await wsClient.command(
            `INSERT test::${tableName} [{ id: 1, name: 'alice' }]`,
            null,
            []
        );

        const reported = () => consoleErrorSpy.mock.calls.some(
            call => String(call[0]).includes('Subscription change could not be delivered')
        );
        await waitUntil(reported);

        expect(reported()).toBe(true);
        expect(insertTracker.getCallCount()).toBe(0);

        // The connection is still answering, which is what swallowing the throw is for.
        const result = await wsClient.query(
            `from test::${tableName}`,
            null,
            [Shape.object({id: Shape.number(), name: Shape.string()})]
        );
        expect(result[0].length).toBe(1);
        expect(result[0][0].id).toBe(1);

        await wsClient.unsubscribe(subscriptionId);

        consoleErrorSpy.mockRestore();
    }, 15000);

    it('should report a malformed type descriptor without abandoning the rest of the message', async () => {
        const tableName = createTestTableName('sub_err_wiretype');
        await createTestTable(wsClient, tableName, [
            'id Int4',
            'name Utf8'
        ]);

        const shape = Shape.object({
            id: Shape.number(),
            name: Shape.string()
        });

        const errors: Error[] = [];
        const insertTracker = createCallbackTracker(shape);
        const updateTracker = createCallbackTracker(shape);

        const subscriptionId = await wsClient.subscribe(
            `from test::${tableName}`,
            null,
            shape,
            {
                onInsert: insertTracker.callback,
                onUpdate: updateTracker.callback,
                onError: (error) => errors.push(error)
            }
        );

        // A bare string where a type descriptor object belongs is what a protocol mismatch looks
        // like. Reading the wire types is per frame, so this is the first frame's problem only; a
        // server cannot be made to send it, so the handler is driven directly.
        const deliver = () => (wsClient as any).handleChangeMessage({
            type: 'Change',
            payload: {
                subscriptionId,
                contentType: CONTENT_TYPE_FRAMES,
                body: {
                    frames: [
                        {
                            op: 1,
                            row_numbers: [1],
                            columns: [
                                {name: 'id', type: 'Int4', payload: ['1']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['alice']}
                            ]
                        },
                        {
                            op: 1,
                            row_numbers: [2],
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['2']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['bob']}
                            ]
                        },
                        {
                            op: 2,
                            row_numbers: [3],
                            columns: [
                                {name: 'id', type: {id: 'Int4'}, payload: ['3']},
                                {name: 'name', type: {id: 'Utf8'}, payload: ['carol']}
                            ]
                        }
                    ]
                }
            }
        });

        // Nothing escapes into the socket handler, where it would stall every later message.
        expect(deliver).not.toThrow();

        expect(errors.length).toBe(1);
        expect(errors[0]).toBeInstanceOf(Error);

        expect(insertTracker.getAllRows()).toEqual([
            {id: 2, name: 'bob', '#rownum': 2}
        ]);
        expect(updateTracker.getAllRows()).toEqual([
            {id: 3, name: 'carol', '#rownum': 3}
        ]);

        await wsClient.unsubscribe(subscriptionId);
    }, 15000);
});
