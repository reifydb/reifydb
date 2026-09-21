// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {StrictMode} from 'react';
import type {ReactNode} from 'react';
import {describe, expect, it} from 'vitest';
import {act, render, renderHook, screen} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import {Store} from '@reifydb/store';
import {StoreProvider, rql, useSubscription} from '../src';
import {FakeClient, flush} from './fake-client';
import {setup} from './support';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const items = rql(shape)`from test::items`;
const other = rql(shape)`from test::others`;

describe('useSubscription', () => {
    it('returns a seeded entry synchronously on first render', () => {
        const {client, store, wrapper} = setup();
        store.seed(items, null, [{id: 1, name: 'a'}]);
        const {result} = renderHook(() => useSubscription(items, null), {wrapper});
        expect(result.current.status).toBe('ready');
        expect(result.current.data).toEqual([{id: 1, name: 'a'}]);
        expect(client.subscribes).toHaveLength(0);
    });

    it('with a fake client it renders loading, then ready after the acknowledgement', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useSubscription(items, null), {wrapper});
        expect(result.current.status).toBe('loading');
        expect(client.subscribes).toHaveLength(1);
        expect(client.subscribes[0].rql).toBe(items.rql);
        await act(async () => {
            client.subscribes[0].resolve('sub-1');
            await flush();
        });
        expect(result.current.status).toBe('ready');
    });

    it('rerendering with equal arguments does not subscribe again', () => {
        const {client, wrapper} = setup();
        const {rerender} = renderHook(
            ({params}: {params: {a: number; b: number}}) => useSubscription(rql(shape)<{a: number; b: number}>`from test::items`, params),
            {wrapper, initialProps: {params: {a: 1, b: 2}}}
        );
        rerender({params: {b: 2, a: 1}});
        expect(client.subscribes).toHaveLength(1);
    });

    it('changing rql releases the old entry and subscribes the new one', async () => {
        const {client, store, wrapper} = setup();
        const {rerender} = renderHook(
            ({spec}: {spec: typeof items}) => useSubscription(spec, null),
            {wrapper, initialProps: {spec: items}}
        );
        await act(async () => {
            client.subscribes[0].resolve('sub-1');
            await flush();
        });
        rerender({spec: other});
        await act(flush);
        expect(client.subscribes.map(call => call.rql)).toEqual([items.rql, other.rql]);
        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1']);
        expect(Object.keys(store.getSnapshot().entries)).toHaveLength(1);
        expect(store.getEntry(other, null).status).toBe('loading');
    });

    it('enabled: false never subscribes and flipping to true subscribes', () => {
        const {client, wrapper} = setup();
        const {result, rerender} = renderHook(
            ({enabled}: {enabled: boolean}) => useSubscription(items, null, {enabled}),
            {wrapper, initialProps: {enabled: false}}
        );
        expect(client.subscribes).toHaveLength(0);
        expect(result.current.status).toBe('loading');
        rerender({enabled: true});
        expect(client.subscribes).toHaveLength(1);
    });

    it('unmount releases', async () => {
        const {client, store, wrapper} = setup();
        const {unmount} = renderHook(() => useSubscription(items, null), {wrapper});
        await act(async () => {
            client.subscribes[0].resolve('sub-1');
            await flush();
        });
        unmount();
        await flush();
        expect(client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1']);
        expect(store.getSnapshot().entries).toEqual({});
    });

    it('under React.StrictMode a mount produces exactly one client subscribe', async () => {
        const client = new FakeClient();
        const store = new Store(client);
        const wrapper = ({children}: {children: ReactNode}) => (
            <StrictMode>
                <StoreProvider store={store}>{children}</StoreProvider>
            </StrictMode>
        );
        const {result} = renderHook(() => useSubscription(items, null), {wrapper});
        await act(async () => {
            client.subscribes[0].resolve('sub-1');
            await flush();
        });
        expect(client.subscribes).toHaveLength(1);
        expect(client.unsubscribes).toHaveLength(0);
        expect(result.current.status).toBe('ready');
    });

    it('two sibling components with the same arguments share one subscribe call and both update from one pushed row', async () => {
        const {client, wrapper} = setup();
        function Items({label}: {label: string}) {
            const entry = useSubscription(items, null);
            return <ul data-testid={label}>{entry.data.map(row => <li key={row.id}>{row.name}</li>)}</ul>;
        }
        render(<><Items label="left"/><Items label="right"/></>, {wrapper});
        expect(client.subscribes).toHaveLength(1);
        act(() => {
            client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        });
        expect(screen.getByTestId('left').textContent).toBe('a');
        expect(screen.getByTestId('right').textContent).toBe('a');
    });

    it('a pushed row into entry A rerenders the consumer of A and not the consumer of B', () => {
        const {client, wrapper} = setup();
        const renders = {a: 0, b: 0};
        function A() {
            renders.a += 1;
            const entry = useSubscription(items, null);
            return <span>{entry.data.length}</span>;
        }
        function B() {
            renders.b += 1;
            const entry = useSubscription(other, null);
            return <span>{entry.data.length}</span>;
        }
        render(<><A/><B/></>, {wrapper});
        const before = {...renders};
        act(() => {
            client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        });
        expect(renders.a).toBe(before.a + 1);
        expect(renders.b).toBe(before.b);
    });
});
