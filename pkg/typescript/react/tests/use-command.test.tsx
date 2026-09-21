// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {act, renderHook} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import type {FrameResults} from '@reifydb/core';
import {rql, useCommand} from '../src';
import {setup} from './support';

const shape = Shape.object({id: Shape.int4()});
const insert = rql.write([shape])`insert test::items [{ id: 1 }]`;

describe('useCommand', () => {
    it('isPending is true while the promise is open and false after', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(insert), {wrapper});
        expect(result.current.isPending).toBe(false);
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run(null);
        });
        expect(result.current.isPending).toBe(true);
        expect(client.commands[0].rql).toBe(insert.rql);
        expect(client.commands[0].params).toBeNull();
        expect(client.commands[0].shapes).toBe(insert.shape);
        await act(async () => {
            client.commands[0].resolve([[]]);
            await promise;
        });
        expect(result.current.isPending).toBe(false);
    });

    it('two overlapping runs keep isPending true until the last one settles', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(insert), {wrapper});
        let first!: Promise<unknown>;
        let second!: Promise<unknown>;
        act(() => {
            first = result.current.run(null);
            second = result.current.run(null);
        });
        expect(result.current.isPending).toBe(true);
        await act(async () => {
            client.commands[0].resolve([[]]);
            await first;
        });
        expect(result.current.isPending).toBe(true);
        await act(async () => {
            client.commands[1].resolve([[]]);
            await second;
        });
        expect(result.current.isPending).toBe(false);
    });

    it('a rejection sets error and the next successful run clears it', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(insert), {wrapper});
        const error = new Error('denied');
        let failing!: Promise<unknown>;
        act(() => {
            failing = result.current.run(null);
        });
        await act(async () => {
            client.commands[0].reject(error);
            await expect(failing).rejects.toBe(error);
        });
        expect(result.current.error).toBe(error);
        expect(result.current.isPending).toBe(false);
        let succeeding!: Promise<unknown>;
        act(() => {
            succeeding = result.current.run(null);
        });
        await act(async () => {
            client.commands[1].resolve([[]]);
            await succeeding;
        });
        expect(result.current.error).toBeUndefined();
    });

    it('the resolved value is the tuple typed by the shapes', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(insert), {wrapper});
        let promise!: Promise<FrameResults<typeof insert.shape>>;
        act(() => {
            promise = result.current.run(null);
        });
        client.commands[0].resolve([[{id: 1}]]);
        const [rows] = await act(() => promise);
        const ids: number[] = rows.map(row => row.id);
        expect(ids).toEqual([1]);
    });
});
