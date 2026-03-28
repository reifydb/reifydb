// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useCommandExecutor, getConnection, clearConnection, Schema} from '../../../src';
import {waitForDatabaseHttp} from '../setup';

describe('useCommandExecutor Hook (HTTP)', () => {
    beforeAll(async () => {
        await waitForDatabaseHttp();
        const conn = getConnection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN});
        await conn.connect();
    }, 30000);


    afterAll(() => {
        clearConnection();
    });

    it('should execute a simple command', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        expect(result.current.isExecuting).toBe(false);
        expect(result.current.results).toBeUndefined();

        act(() => {
            result.current.command(
                `MAP {answer: 42}`,
                undefined,
                [Schema.object({ answer: Schema.number() })]
            );
        });

        expect(result.current.isExecuting).toBe(true);

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.error).toBeUndefined();
        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows).toHaveLength(1);
        expect(result.current.results![0].rows[0]).toEqual({answer: 42});
    });

    it('should execute multiple statements', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command(
                [
                    `MAP {first: 1}`,
                    `MAP {second: 2}`,
                    `MAP {third: 3}`
                ],
                undefined,
                [
                    Schema.object({ first: Schema.number() }),
                    Schema.object({ second: Schema.number() }),
                    Schema.object({ third: Schema.number() })
                ]
            );
        });

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.results).toHaveLength(3);
        expect(result.current.results![0].rows[0]).toEqual({first: 1});
        expect(result.current.results![1].rows[0]).toEqual({second: 2});
        expect(result.current.results![2].rows[0]).toEqual({third: 3});
    });

    it('should handle command with parameters', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command(
                `MAP {result: $value}`,
                {value: 'test_string'},
                [Schema.object({ result: Schema.string() })]
            );
        });

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.results![0].rows[0]).toEqual({result: 'test_string'});
    });

    it('should handle command errors', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command('INVALID SYNTAX HERE');
        });

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
            expect(result.current.error).toBeDefined();
        });

        expect(result.current.results).toBeUndefined();
        expect(result.current.error).toBeDefined();
    });

    it('should cancel previous command when new one starts', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command(
                `MAP {first: 1}`,
                undefined,
                [Schema.object({ first: Schema.number() })]
            );
        });

        act(() => {
            result.current.command(
                `MAP {second: 2}`,
                undefined,
                [Schema.object({ second: Schema.number() })]
            );
        });

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
        });

        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows[0]).toEqual({second: 2});
    });

    it('should handle empty results', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command(`FROM [{x:1}] FILTER x > 10`);
        });

        await waitFor(() => {
            expect(result.current.isExecuting).toBe(false);
        });

        expect(result.current.error).toBeUndefined();
        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows).toHaveLength(0);
        expect(result.current.results![0].columns).toHaveLength(0);
    });

    it('should handle concurrent hook instances', async () => {
        const {result: result1} = renderHook(() => useCommandExecutor());
        const {result: result2} = renderHook(() => useCommandExecutor());

        act(() => {
            result1.current.command(
                `MAP {value: 100}`,
                undefined,
                [Schema.object({ value: Schema.number() })]
            );
            result2.current.command(
                `MAP {value: 200}`,
                undefined,
                [Schema.object({ value: Schema.number() })]
            );
        });

        await waitFor(() => {
            expect(result1.current.isExecuting).toBe(false);
            expect(result2.current.isExecuting).toBe(false);
        });

        expect(result1.current.results![0].rows[0]).toEqual({value: 100});
        expect(result2.current.results![0].rows[0]).toEqual({value: 200});
    });

    it('should support manual command cancellation', async () => {
        const {result} = renderHook(() => useCommandExecutor());

        act(() => {
            result.current.command(
                `MAP {test: 1}`,
                undefined,
                [Schema.object({ test: Schema.number() })]
            );
        });

        expect(result.current.isExecuting).toBe(true);

        act(() => {
            result.current.cancelCommand();
        });

        expect(result.current.isExecuting).toBe(false);
        expect(result.current.error).toBe('Command cancelled');
    });
});
