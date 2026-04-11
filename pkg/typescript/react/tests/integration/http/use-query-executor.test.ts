// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, act, waitFor} from '@testing-library/react';
import {useQueryExecutor, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database_http} from '../setup';

describe('useQueryExecutor Hook (HTTP)', () => {
    beforeAll(async () => {
        await wait_for_database_http();
        const conn = get_connection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN});
        await conn.connect();
    }, 30000);


    afterAll(() => {
        clear_connection();
    });

    it('should execute a simple query', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        expect(result.current.is_executing).toBe(false);
        expect(result.current.results).toBeUndefined();

        act(() => {
            result.current.query(
                `MAP {answer: 42}`,
                undefined,
                [Shape.object({ answer: Shape.number() })]
            );
        });

        expect(result.current.is_executing).toBe(true);

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.error).toBeUndefined();
        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows).toHaveLength(1);
        expect(result.current.results![0].rows[0]).toEqual({answer: 42});
    });

    it('should execute multiple statements', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query(
                [
                    `MAP {first: 1}`,
                    `MAP {second: 2}`,
                    `MAP {third: 3}`
                ],
                undefined,
                [
                    Shape.object({ first: Shape.number() }),
                    Shape.object({ second: Shape.number() }),
                    Shape.object({ third: Shape.number() })
                ]
            );
        });

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.results).toHaveLength(3);
        expect(result.current.results![0].rows[0]).toEqual({first: 1});
        expect(result.current.results![1].rows[0]).toEqual({second: 2});
        expect(result.current.results![2].rows[0]).toEqual({third: 3});
    });

    it('should handle query with parameters', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query(
                `MAP {result: $value}`,
                {value: 'test_string'},
                [Shape.object({ result: Shape.string() })]
            );
        });

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
            expect(result.current.results).toBeDefined();
        });

        expect(result.current.results![0].rows[0]).toEqual({result: 'test_string'});
    });

    it('should handle query errors', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query('INVALID SYNTAX HERE');
        });

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
            expect(result.current.error).toBeDefined();
        });

        expect(result.current.results).toBeUndefined();
        expect(result.current.error).toBeDefined();
    });

    it('should cancel previous query when new one starts', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query(
                `MAP {first: 1}`,
                undefined,
                [Shape.object({ first: Shape.number() })]
            );
        });

        act(() => {
            result.current.query(
                `MAP {second: 2}`,
                undefined,
                [Shape.object({ second: Shape.number() })]
            );
        });

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
        });

        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows[0]).toEqual({second: 2});
    });

    it('should handle empty results', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query(`FROM [{x:1}] FILTER x > 10`);
        });

        await waitFor(() => {
            expect(result.current.is_executing).toBe(false);
        });

        expect(result.current.error).toBeUndefined();
        expect(result.current.results).toHaveLength(1);
        expect(result.current.results![0].rows).toHaveLength(0);
        expect(result.current.results![0].columns).toHaveLength(0);
    });

    it('should handle concurrent hook instances', async () => {
        const {result: result1} = renderHook(() => useQueryExecutor());
        const {result: result2} = renderHook(() => useQueryExecutor());

        act(() => {
            result1.current.query(
                `MAP {value: 100}`,
                undefined,
                [Shape.object({ value: Shape.number() })]
            );
            result2.current.query(
                `MAP {value: 200}`,
                undefined,
                [Shape.object({ value: Shape.number() })]
            );
        });

        await waitFor(() => {
            expect(result1.current.is_executing).toBe(false);
            expect(result2.current.is_executing).toBe(false);
        });

        expect(result1.current.results![0].rows[0]).toEqual({value: 100});
        expect(result2.current.results![0].rows[0]).toEqual({value: 200});
    });

    it('should support manual query cancellation', async () => {
        const {result} = renderHook(() => useQueryExecutor());

        act(() => {
            result.current.query(
                `MAP {test: 1}`,
                undefined,
                [Shape.object({ test: Shape.number() })]
            );
        });

        expect(result.current.is_executing).toBe(true);

        act(() => {
            result.current.cancel_query();
        });

        expect(result.current.is_executing).toBe(false);
        expect(result.current.error).toBe('Query cancelled');
    });
});
