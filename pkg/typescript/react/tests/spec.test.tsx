// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {act, renderHook} from '@testing-library/react';
import {DurationValue, Shape} from '@reifydb/core';
import {rql, useAdmin, useCommand, useQuery, useSubscription} from '../src';
import {flush} from './fake-client';
import {setup} from './support';

const item = Shape.object({id: Shape.int4(), name: Shape.utf8()});
const total = Shape.object({total: Shape.int8()});
const config = {linger: DurationValue.parse('5s')};
const byId = rql(item)<{id: number}>`from test::items filter { id == $id }`;
const pair = rql([item, total])`output from test::items; output from test::totals`;

describe('hook specs', () => {
    it('useSubscription hands the spec rql, params, shape and config to the client unchanged', () => {
        // The hook no longer takes config itself, so a spec config it dropped would never reach the server.
        const {client, wrapper} = setup();
        renderHook(() => useSubscription(byId.options({config}), {id: 7}), {wrapper});
        expect(client.subscribes).toHaveLength(1);
        expect(client.subscribes[0].rql).toBe('from test::items filter { id == $id }');
        expect(client.subscribes[0].params).toEqual({id: 7});
        expect(client.subscribes[0].shape).toBe(item);
        expect(client.subscribes[0].config).toBe(config);
    });

    it('useQuery hands a one shape spec as a one element tuple and a tuple spec as the tuple itself', () => {
        // The client pairs frames with shapes by position, so the shapes it gets must be exactly the frames asked for.
        const {client, wrapper} = setup();
        renderHook(() => useQuery(byId, {id: 3}), {wrapper});
        renderHook(() => useQuery(pair, null), {wrapper});
        expect(client.queries[0].rql).toBe('from test::items filter { id == $id }');
        expect(client.queries[0].params).toEqual({id: 3});
        expect(client.queries[0].shapes).toEqual([item]);
        expect(client.queries[1].rql).toBe('output from test::items; output from test::totals');
        expect(client.queries[1].params).toBeNull();
        expect(client.queries[1].shapes).toEqual([item, total]);
    });

    it('useCommand and useAdmin run the spec rql, params and shapes on their own channel', async () => {
        // The hook picks the rights, so a spec run on the wrong channel would get the wrong ones.
        const {client, wrapper} = setup();
        const spec = rql.write([item, total])<{id: number}>`insert test::items [{ id: $id }]; output from test::items; output from test::totals`;
        const {result} = renderHook(() => ({command: useCommand(spec), admin: useAdmin(spec)}), {wrapper});
        await act(async () => {
            result.current.command.run({id: 4});
            result.current.admin.run({id: 5});
        });
        expect(client.commands).toHaveLength(1);
        expect(client.admins).toHaveLength(1);
        expect(client.commands[0].rql).toBe(spec.rql);
        expect(client.commands[0].params).toEqual({id: 4});
        expect(client.commands[0].shapes).toEqual([item, total]);
        expect(client.admins[0].rql).toBe(spec.rql);
        expect(client.admins[0].params).toEqual({id: 5});
        expect(client.admins[0].shapes).toEqual([item, total]);
    });

    it('a tuple useQuery holds one array per frame while loading and once ready', async () => {
        // A component destructures data by frame on every render, so the tuple layout must hold before the result arrives.
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useQuery(pair, null), {wrapper});
        expect(result.current.status).toBe('loading');
        expect(result.current.data).toEqual([[], []]);
        await act(async () => {
            client.queries[0].resolve([[{'#rownum': 1, id: 1, name: 'a'}], [{'#rownum': 1, total: 1n}]]);
            await flush();
        });
        expect(result.current.status).toBe('ready');
        expect(result.current.data).toEqual([[{id: 1, name: 'a'}], [{total: 1n}]]);
    });

    it('a failed tuple useQuery holds one array per frame alongside the error', async () => {
        // An error render still reads data by frame, so losing the tuple layout would crash the error path.
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useQuery(pair, null), {wrapper});
        const error = new Error('denied');
        await act(async () => {
            client.queries[0].reject(error);
            await flush();
        });
        expect(result.current.status).toBe('error');
        expect(result.current.error).toBe(error);
        expect(result.current.data).toEqual([[], []]);
    });
});
