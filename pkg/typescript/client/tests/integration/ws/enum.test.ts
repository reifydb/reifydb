// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, it, expect, beforeAll, beforeEach, afterEach } from 'vitest';
import { Client, WsClient } from '../../../src';
import { Shape, EnumVariant } from '@reifydb/core';
import { waitForDatabase } from '../setup';
import { createTestTableName, createCallbackTracker } from './subscription-helpers';

describe('WebSocket enums', () => {
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

    async function enumTable(prefix: string): Promise<string> {
        const ns = createTestTableName(prefix);
        await wsClient.admin(`create namespace ${ns}`, null, []);
        await wsClient.admin(`create enum ${ns}::status { Todo, Doing, Done }`, null, []);
        await wsClient.admin(`create table ${ns}::t { id: int4, status: ${ns}::status }`, null, []);
        return ns;
    }

    it('a_query_reads_an_enum_column_as_its_variant', async () => {
        // A client must get the name, never the stored tag, both raw and through a text shape.
        const ns = await enumTable('enum_query');
        await wsClient.command(`INSERT ${ns}::t [{ id: 1, status: 'doing' }]`, null, []);
        const raw = await wsClient.query(`FROM ${ns}::t MAP { status }`, null, []);
        expect(raw[0][0].status).toEqual(new EnumVariant(`${ns}::status`, 'doing'));
        const shaped = await wsClient.query(`FROM ${ns}::t MAP { status }`, null, [Shape.object({ status: Shape.string() })]);
        expect(shaped[0][0].status).toBe('doing');
    }, 10000);

    it('a_let_variable_enum_field_reads_and_compares_by_name', async () => {
        // The factory procedure shape: read a status into a variable, compare with any case, write it as text.
        const ns = await enumTable('enum_let');
        await wsClient.command(`INSERT ${ns}::t [{ id: 1, status: 'doing' }]`, null, []);
        const frames = await wsClient.command(
            `let $t = from ${ns}::t filter { id == 1 } map { id, status }; MAP { got: $t.status, same: $t.status == 'DOING', other: $t.status in ['todo', 'done'] }`,
            null,
            [Shape.object({ got: Shape.string(), same: Shape.boolean(), other: Shape.boolean() })]
        );
        expect(frames[frames.length - 1][0]).toEqual({ got: 'doing', same: true, other: false });
    }, 10000);

    it('a_subscription_delivers_enum_names_for_inserts_and_updates', async () => {
        // A live client reads the same status column a query gives; a tag column would leave it empty.
        const ns = await enumTable('enum_sub');
        const shape = Shape.object({ id: Shape.number(), status: Shape.string() });
        const inserts = createCallbackTracker(shape);
        const updates = createCallbackTracker(shape);
        const [subscriptionId] = await wsClient.subscribe([{
            rql: `from ${ns}::t`,
            params: null,
            shape,
            callbacks: { onInsert: inserts.callback, onUpdate: updates.callback }
        }]);

        await wsClient.command(`INSERT ${ns}::t [{ id: 1, status: 'todo' }]`, null, []);
        const inserted = await inserts.waitForCall();
        expect(inserted).toHaveLength(1);
        expect(inserted[0]).toMatchObject({ id: 1, status: 'todo' });

        await wsClient.command(`UPDATE ${ns}::t { status: 'done' } FILTER { id == 1 }`, null, []);
        const updated = await updates.waitForCall();
        expect(updated).toHaveLength(1);
        expect(updated[0]).toMatchObject({ id: 1, status: 'done' });

        await wsClient.unsubscribe([subscriptionId]);
    }, 10000);
});
