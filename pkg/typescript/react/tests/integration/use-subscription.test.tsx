// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {StrictMode} from 'react';
import {render, screen, waitFor} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '@reifydb/store';
import {StoreProvider, useSubscription} from '../../src';
import {connect, namespace, poll} from './setup';

const ns = namespace('use_sub_it');
const items = Shape.object({id: Shape.int4(), name: Shape.utf8()});
const subscriptionRow = Shape.object({id: Shape.uint8()});

let counter = 0;

describe('useSubscription against a live server', () => {
    let client: WsClient;
    let store: Store;

    async function table(prefix: string, columns = 'id: int4, name: utf8'): Promise<string> {
        counter += 1;
        const name = `${ns}::${prefix}_${counter}`;
        await client.admin(`create table ${name} { ${columns} }`, null, []);
        return name;
    }

    async function subscriptionCount(): Promise<number> {
        const [rows] = await client.query('from system::subscriptions', null, [subscriptionRow]);
        return rows.length;
    }

    beforeAll(async () => {
        client = await connect();
        await client.admin(`create namespace ${ns}`, null, []);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    it('renders loading first and then ready once the server acknowledges', async () => {
        const name = await table('ready');
        function View() {
            const entry = useSubscription(`from ${name}`, null, items);
            return <div data-testid="status">{entry.status}</div>;
        }
        render(<StoreProvider store={store}><View/></StoreProvider>);
        expect(screen.getByTestId('status').textContent).toBe('loading');
        await waitFor(() => expect(screen.getByTestId('status').textContent).toBe('ready'));
    });

    it('an insert, an update and a delete each reach the rendered output', async () => {
        const name = await table('ops');
        function View() {
            const entry = useSubscription(`from ${name}`, null, items);
            return <ul data-testid={entry.status}>{entry.data.map(row => <li key={row.id}>{row.name}</li>)}</ul>;
        }
        render(<StoreProvider store={store}><View/></StoreProvider>);
        await screen.findByTestId('ready');

        await client.command(`insert ${name} [{ id: 1, name: 'a' }]`, null, []);
        await screen.findByText('a');

        await client.command(`update ${name} { name: 'b' } filter id == 1`, null, []);
        await screen.findByText('b');
        await waitFor(() => expect(screen.queryByText('a')).toBeNull());

        await client.command(`delete ${name} filter id == 1`, null, []);
        await waitFor(() => expect(screen.queryByText('b')).toBeNull());
    });

    it('unmounting removes the subscription from the server', async () => {
        const name = await table('unmount');
        function View() {
            const entry = useSubscription(`from ${name}`, null, items);
            return <div data-testid={entry.status}/>;
        }
        const before = await subscriptionCount();
        const {unmount} = render(<StoreProvider store={store}><View/></StoreProvider>);
        await screen.findByTestId('ready');
        expect(await subscriptionCount()).toBe(before + 1);
        unmount();
        await poll(async () => (await subscriptionCount()) === before);
    });

    it('under StrictMode a mount still leaves exactly one server subscription', async () => {
        const name = await table('strict');
        function View() {
            const entry = useSubscription(`from ${name}`, null, items);
            return <div data-testid={entry.status}/>;
        }
        const before = await subscriptionCount();
        const {unmount} = render(
            <StrictMode><StoreProvider store={store}><View/></StoreProvider></StrictMode>
        );
        await screen.findByTestId('ready');
        expect(await subscriptionCount()).toBe(before + 1);
        unmount();
        await poll(async () => (await subscriptionCount()) === before);
    });

    it('two components on the same query share one server subscription and both see the row', async () => {
        const name = await table('shared');
        function View({tag}: {tag: string}) {
            const entry = useSubscription(`from ${name}`, null, items);
            return <div data-testid={tag}>{entry.data.map(row => row.name).join(',')}</div>;
        }
        const before = await subscriptionCount();
        const {unmount} = render(
            <StoreProvider store={store}><View tag="one"/><View tag="two"/></StoreProvider>
        );
        await waitFor(async () => expect(await subscriptionCount()).toBe(before + 1));
        await client.command(`insert ${name} [{ id: 1, name: 'shared' }]`, null, []);
        await waitFor(() => {
            expect(screen.getByTestId('one').textContent).toBe('shared');
            expect(screen.getByTestId('two').textContent).toBe('shared');
        });
        unmount();
        await poll(async () => (await subscriptionCount()) === before);
    });

    it('a subscription to a missing table renders the error status', async () => {
        function View() {
            const entry = useSubscription(`from ${ns}::not_there`, null, items);
            return <div data-testid="status">{entry.status}</div>;
        }
        render(<StoreProvider store={store}><View/></StoreProvider>);
        await waitFor(() => expect(screen.getByTestId('status').textContent).toBe('error'));
    });

    it('enabled false never subscribes on the server', async () => {
        const name = await table('disabled');
        function View() {
            const entry = useSubscription(`from ${name}`, null, items, {enabled: false});
            return <div data-testid="status">{entry.status}</div>;
        }
        const before = await subscriptionCount();
        render(<StoreProvider store={store}><View/></StoreProvider>);
        await new Promise(resolve => setTimeout(resolve, 500));
        expect(await subscriptionCount()).toBe(before);
        expect(screen.getByTestId('status').textContent).toBe('loading');
    });

    it('primitive and value object shapes both decode through the hook', async () => {
        const name = await table('shapes', 'id: int4, big: int8');
        const primitive = Shape.object({id: Shape.int4(), big: Shape.int8()});
        const wrapped = Shape.object({id: Shape.int4(), big: Shape.int8Value()});
        function View() {
            const plain = useSubscription(`from ${name}`, null, primitive);
            const value = useSubscription(`from ${name}`, null, wrapped);
            return (
                <div>
                    <span data-testid="plain">{plain.data.map(r => typeof r.big).join(',')}</span>
                    <span data-testid="value">{value.data.map(r => r.big.type).join(',')}</span>
                </div>
            );
        }
        render(<StoreProvider store={store}><View/></StoreProvider>);
        await client.command(`insert ${name} [{ id: 1, big: 9007199254740993 }]`, null, []);
        await waitFor(() => {
            expect(screen.getByTestId('plain').textContent).toBe('bigint');
            expect(screen.getByTestId('value').textContent).toBe('Int8');
        });
    });
});
