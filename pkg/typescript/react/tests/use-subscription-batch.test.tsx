// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {StrictMode} from 'react';
import {describe, expect, it} from 'vitest';
import {act, render, screen} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import {useSubscription} from '../src';
import {flush} from './fake-client';
import {setupBatching, withStore} from './support';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const monitors = 'from test::monitors';
const results = 'from test::results';
const regions = 'from test::regions';

function Page() {
    const a = useSubscription(monitors, null, shape)
    const b = useSubscription(results, null, shape)
    const c = useSubscription(regions, null, shape)
    return <div data-testid="status">{`${a.status}/${b.status}/${c.status}`}</div>
}

describe('useSubscription against a batching store', () => {
    it('opens one batch for every hook a page mounts', async () => {
        // This is what batching is for. React runs the three mount effects in one pass, so the page
        // that used to cost three round trips costs one, without a hook having to know about it.
        const {client, store} = setupBatching();
        render(<Page />, {wrapper: withStore(store)});
        await act(async () => {
            await flush();
        });

        expect(client.subscribes).toHaveLength(0);
        expect(client.batches).toHaveLength(1);
        expect(client.batches[0].members.map(member => member.rql)).toEqual([monitors, results, regions]);
    });

    it('turns the whole page ready off one ack', async () => {
        const {client, store} = setupBatching();
        render(<Page />, {wrapper: withStore(store)});
        await act(async () => {
            await flush();
        });
        expect(screen.getByTestId('status').textContent).toBe('loading/loading/loading');

        await act(async () => {
            client.batches[0].resolve({
                batchId: 'batch-1',
                subscriptionIds: ['sub-monitors', 'sub-results', 'sub-regions']
            });
            await flush();
        });

        expect(screen.getByTestId('status').textContent).toBe('ready/ready/ready');
    });

    it('delivers a change to the hook whose rql produced it', async () => {
        // Every hook reads its own entry, so a batch that handed the rows to the wrong member would
        // render one panel's data inside another.
        const {client, store} = setupBatching();
        render(<Page />, {wrapper: withStore(store)});
        await act(async () => {
            await flush();
        });
        await act(async () => {
            client.batches[0].resolve({
                batchId: 'batch-1',
                subscriptionIds: ['sub-monitors', 'sub-results', 'sub-regions']
            });
            await flush();
        });

        act(() => {
            client.batches[0].members[1].callbacks.onInsert?.([{'#rownum': 1, id: 7, name: 'r'}]);
        });

        expect(store.getEntry(monitors, null, shape).data).toEqual([]);
        expect(store.getEntry(results, null, shape).data).toEqual([{id: 7, name: 'r'}]);
    });

    it('marks the whole page in error when the batch is refused', async () => {
        // A page whose subscriptions all failed has to say so. One shared ack means one shared
        // failure, and an entry left loading would be a spinner that never resolves.
        const {client, store} = setupBatching();
        render(<Page />, {wrapper: withStore(store)});
        await act(async () => {
            await flush();
        });

        await act(async () => {
            client.batches[0].reject(new Error('denied'));
            await flush();
        });

        expect(screen.getByTestId('status').textContent).toBe('error/error/error');
    });

    it('opens one batch under strict mode, not one per mount pass', async () => {
        // Strict mode mounts, unmounts and remounts every effect. Each pass has to close its own
        // batch, and the subscriptions the discarded pass opened have to be given back.
        const {client, store} = setupBatching();
        render(
            <StrictMode>
                <Page />
            </StrictMode>,
            {wrapper: withStore(store)}
        );
        await act(async () => {
            await flush();
        });

        expect(client.batches).toHaveLength(1);
        expect(client.batches[0].members).toHaveLength(3);
    });
});
