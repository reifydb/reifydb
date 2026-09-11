// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {act, renderHook} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import {useQuery} from '../src';
import {flush} from './fake-client';
import {setup} from './support';

const shape = Shape.object({id: Shape.int4(), name: Shape.string()});
const rql = 'from test::items';

describe('useQuery', () => {
    it('runs once per key and exposes loading then ready', async () => {
        const {client, wrapper} = setup();
        const {result, rerender} = renderHook(
            ({params}: {params: {a: number}}) => useQuery(rql, params, shape),
            {wrapper, initialProps: {params: {a: 1}}}
        );
        expect(result.current.status).toBe('loading');
        rerender({params: {a: 1}});
        expect(client.queries).toHaveLength(1);
        expect(client.queries[0].rql).toBe(rql);
        expect(client.queries[0].shapes).toEqual([shape]);
        await act(async () => {
            client.queries[0].resolve([[{id: 1, name: 'a'}]]);
            await flush();
        });
        expect(result.current.status).toBe('ready');
        expect(result.current.data).toEqual([{id: 1, name: 'a'}]);
        expect(client.queries).toHaveLength(1);
    });

    it('exposes error when the query rejects', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useQuery(rql, null, shape), {wrapper});
        const error = new Error('denied');
        await act(async () => {
            client.queries[0].reject(error);
            await flush();
        });
        expect(result.current.status).toBe('error');
        expect(result.current.error).toBe(error);
    });

    it('enabled: false does not run', () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useQuery(rql, null, shape, {enabled: false}), {wrapper});
        expect(client.queries).toHaveLength(0);
        expect(result.current.status).toBe('loading');
    });
});
