// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {act, renderHook} from '@testing-library/react';
import {Shape} from '@reifydb/core';
import type {FrameResults} from '@reifydb/core';
import {useCommand} from '../src';
import {setup} from './support';

const shape = Shape.object({id: Shape.int4()});
const shapes = [shape] as const;
const rql = "insert test::items [{ id: 1 }]";

describe('useCommand', () => {
    it('isPending is true while the promise is open and false after', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(shapes), {wrapper});
        expect(result.current.isPending).toBe(false);
        let promise!: Promise<unknown>;
        act(() => {
            promise = result.current.run(rql);
        });
        expect(result.current.isPending).toBe(true);
        expect(client.commands[0].rql).toBe(rql);
        expect(client.commands[0].params).toBeNull();
        expect(client.commands[0].shapes).toBe(shapes);
        await act(async () => {
            client.commands[0].resolve([[]]);
            await promise;
        });
        expect(result.current.isPending).toBe(false);
    });

    it('two overlapping runs keep isPending true until the last one settles', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(shapes), {wrapper});
        let first!: Promise<unknown>;
        let second!: Promise<unknown>;
        act(() => {
            first = result.current.run(rql);
            second = result.current.run(rql, {n: 2});
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
        const {result} = renderHook(() => useCommand(shapes), {wrapper});
        const error = new Error('denied');
        let failing!: Promise<unknown>;
        act(() => {
            failing = result.current.run(rql);
        });
        await act(async () => {
            client.commands[0].reject(error);
            await expect(failing).rejects.toBe(error);
        });
        expect(result.current.error).toBe(error);
        expect(result.current.isPending).toBe(false);
        let succeeding!: Promise<unknown>;
        act(() => {
            succeeding = result.current.run(rql);
        });
        await act(async () => {
            client.commands[1].resolve([[]]);
            await succeeding;
        });
        expect(result.current.error).toBeUndefined();
    });

    it('the resolved value is the tuple typed by the shapes', async () => {
        const {client, wrapper} = setup();
        const {result} = renderHook(() => useCommand(shapes), {wrapper});
        let promise!: Promise<FrameResults<typeof shapes>>;
        act(() => {
            promise = result.current.run(rql);
        });
        client.commands[0].resolve([[{id: 1}]]);
        const [rows] = await act(() => promise);
        const ids: number[] = rows.map(row => row.id);
        expect(ids).toEqual([1]);
    });
});
