// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {act, renderHook} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import type {FrameResults} from '@reifydb/core';
import {rql, useAdmin} from '../src';
import {setup} from './support';

const shape = Shape.object({name: Shape.utf8()});
const ddl = rql.write([shape])`create table test::items { id: int4 }`;
const createNamespace = rql.write([shape])<{name: string}>`create namespace $name`;
const namespaces = rql.write([shape])`from system::namespaces`;

describe('useAdmin', () => {
    it('runs nothing on mount, so DDL only reaches the server when run is called', () => {
        const {client, wrapper} = setup();
        const {rerender} = renderHook(() => useAdmin(ddl), {wrapper});
        rerender();
        expect(client.admins).toHaveLength(0);
        expect(client.commands).toHaveLength(0);
        expect(client.queries).toHaveLength(0);
    });

    it('run goes to the admin channel, not command or query, because DDL needs the admin rights', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(ddl), {wrapper});
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run(null);
        });
        expect(client.admins).toHaveLength(1);
        expect(client.commands).toHaveLength(0);
        expect(client.queries).toHaveLength(0);
        expect(client.admins[0].rql).toBe(ddl.rql);
        expect(client.admins[0].params).toBeNull();
        expect(client.admins[0].shapes).toBe(ddl.shape);
        await act(async () => {
            client.admins[0].resolve([[]]);
            await promise;
        });
    });

    it('passes params through unchanged', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(createNamespace), {wrapper});
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run({name: 'app'});
        });
        expect(client.admins[0].params).toEqual({name: 'app'});
        await act(async () => {
            client.admins[0].resolve([[]]);
            await promise;
        });
    });

    it('isPending is true while the promise is open and false after', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(ddl), {wrapper});
        expect(result.current.isPending).toBe(false);
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run(null);
        });
        expect(result.current.isPending).toBe(true);
        await act(async () => {
            client.admins[0].resolve([[]]);
            await promise;
        });
        expect(result.current.isPending).toBe(false);
    });

    it('two overlapping runs keep isPending true until the last one settles', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(ddl), {wrapper});
        let first!: Promise<unknown>;
        let second!: Promise<unknown>;
        act(() => {
            first = result.current.run(null);
            second = result.current.run(null);
        });
        expect(result.current.isPending).toBe(true);
        await act(async () => {
            client.admins[0].resolve([[]]);
            await first;
        });
        expect(result.current.isPending).toBe(true);
        await act(async () => {
            client.admins[1].resolve([[]]);
            await second;
        });
        expect(result.current.isPending).toBe(false);
    });

    it('a rejection sets error and rethrows, so a refused DDL cannot be read as success', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(ddl), {wrapper});
        const error = new Error('table already exists');
        let failing!: Promise<unknown>;
        act(() => {
            failing = result.current.run(null);
        });
        await act(async () => {
            client.admins[0].reject(error);
            await expect(failing).rejects.toBe(error);
        });
        expect(result.current.error).toBe(error);
        expect(result.current.isPending).toBe(false);
        let succeeding!: Promise<unknown>;
        act(() => {
            succeeding = result.current.run(null);
        });
        await act(async () => {
            client.admins[1].resolve([[]]);
            await succeeding;
        });
        expect(result.current.error).toBeUndefined();
    });

    it('the resolved value is the tuple typed by the shapes, so admin reads decode like a query', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(namespaces), {wrapper});
        let promise!: Promise<FrameResults<typeof namespaces.shape>>;
        act(() => {
            promise = result.current.run(null);
        });
        client.admins[0].resolve([[{name: 'test'}]]);
        const [rows] = await act(() => promise);
        const names: string[] = rows.map(row => row.name);
        expect(names).toEqual(['test']);
    });

    it('creates no store entry, so an admin result is never served to useQuery as cached rows', async () => {
        const {client, store, wrapper} = setup();
        const {result} = renderHook(() => useAdmin(namespaces), {wrapper});
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run(null);
        });
        await act(async () => {
            client.admins[0].resolve([[{name: 'test'}]]);
            await promise;
        });
        expect(store.getSnapshot().entries).toEqual({});
    });
});
