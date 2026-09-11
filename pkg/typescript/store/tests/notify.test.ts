// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import {Store} from '../src';
import {FakeClient} from './fake-client';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items';

describe('notifications', () => {
    it('one listener notification per delivered batch, however many rows it holds', () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.subscribe(rql, null, shape);
        let notified = 0;
        store.subscribeState(() => {
            notified += 1;
        });
        const rows = Array.from({length: 50}, (_, i) => ({'#rownum': i, id: i, name: `row ${i}`}));
        client.subscribes[0].callbacks.onInsert?.(rows);
        expect(notified).toBe(1);
        expect(store.getEntry(rql, null, shape).data).toHaveLength(50);
    });

    it('remove of an unknown rownum does not notify listeners', () => {
        const client = new FakeClient();
        const store = new Store(client);
        store.subscribe(rql, null, shape);
        client.subscribes[0].callbacks.onInsert?.([{'#rownum': 1, id: 1, name: 'a'}]);
        let notified = 0;
        store.subscribeState(() => {
            notified += 1;
        });
        client.subscribes[0].callbacks.onRemove?.([{'#rownum': 99, id: 9, name: 'z'}]);
        expect(notified).toBe(0);
    });
});
