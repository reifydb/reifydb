// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it, vi} from 'vitest';
import {act, render, renderHook} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import {StoreProvider, useStore, useSubscription} from '../src';
import {flush} from './fake-client';
import {setup} from './support';

const shape = Shape.object({id: Shape.int4()});
const rql = 'from test::items';

describe('StoreProvider', () => {
    it('a hook outside the provider throws an error that names the provider', () => {
        // React reports the render error through console.error before rethrowing it.
        const silenced = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        try {
            expect(() => renderHook(() => useStore())).toThrow('StoreProvider');
        } finally {
            silenced.mockRestore();
        }
    });

    it('swapping the store prop releases on the old store and subscribes on the new one', async () => {
        const first = setup();
        const second = setup();
        function Consumer() {
            const entry = useSubscription(rql, null, shape);
            return <span>{entry.status}</span>;
        }
        const {container, rerender} = render(<StoreProvider store={first.store}><Consumer/></StoreProvider>);
        await act(async () => {
            first.client.subscribes[0].resolve('sub-1');
            await flush();
        });
        expect(container.textContent).toBe('ready');
        rerender(<StoreProvider store={second.store}><Consumer/></StoreProvider>);
        await act(flush);
        expect(first.client.unsubscribes.map(call => call.subscriptionId)).toEqual(['sub-1']);
        expect(first.store.getSnapshot().entries).toEqual({});
        expect(second.client.subscribes).toHaveLength(1);
        expect(container.textContent).toBe('loading');
    });
});
