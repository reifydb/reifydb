// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {render, screen, waitFor} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '@reifydb/store';
import {StoreProvider, useSubscription} from '../../src';
import {connect, namespace, poll} from './setup';

const ns = namespace('react_it');
const table = `${ns}::items`;
const items = Shape.object({id: Shape.int4(), name: Shape.string()});
const subscriptionRow = Shape.object({id: Shape.uint8()});

async function subscriptionCount(client: WsClient): Promise<number> {
    const [rows] = await client.query('from system::subscriptions', null, [subscriptionRow]);
    return rows.length;
}

function Items() {
    const entry = useSubscription(`from ${table}`, null, items);
    return <ul data-testid={entry.status}>{entry.data.map(row => <li key={row.id}>{row.name}</li>)}</ul>;
}

describe('react against a live server', () => {
    let client: WsClient;
    let store: Store;

    beforeAll(async () => {
        client = await connect();
        await client.admin(`create namespace ${ns}`, null, []);
        await client.admin(`create table ${table} { id: int4, name: utf8 }`, null, []);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    it('a component subscribes, a command inserts and updates, and unmount removes the server subscription', async () => {
        const before = await subscriptionCount(client);
        const {unmount} = render(<StoreProvider store={store}><Items/></StoreProvider>);
        await screen.findByTestId('ready');
        expect(await subscriptionCount(client)).toBe(before + 1);

        await store.command(`insert ${table} [{ id: 1, name: 'a' }]`, null, []);
        await screen.findByText('a');

        await store.command(`update ${table} { name: 'b' } filter id == 1`, null, []);
        await screen.findByText('b');
        await waitFor(() => expect(screen.queryByText('a')).toBeNull());

        unmount();
        await poll(async () => (await subscriptionCount(client)) === before);
    });
});
