/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useQueryOne, useQueryMany, connection, Schema} from '../../../src';
import {waitForDatabase} from '../setup';

describe('useQuery Hooks', () => {
    beforeAll(async () => {
        await waitForDatabase();
        // Ensure we're connected before tests
        await connection.connect();
    }, 30000);

    afterEach(() => {
        // Don't disconnect between tests to maintain stable connection
    });

    afterAll(() => {
        // Disconnect after all tests
        connection.disconnect();
    });

    describe('useQueryOne', () => {
        it('should execute a single query and return one result', async () => {
            const schema = Schema.object({answer: Schema.number()});
            const {result} = renderHook(() =>
                useQueryOne(
                    `MAP {answer: 42}`,
                    undefined,
                    schema
                )
            );

            expect(result.current.isExecuting).toBe(true);
            expect(result.current.result).toBeUndefined();

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.result).toBeDefined();
            });

            expect(result.current.error).toBeUndefined();
            expect(result.current.result!.rows).toHaveLength(1);
            expect(result.current.result!.rows[0]).toEqual({answer: 42});
        });

        it('should handle parameters', async () => {
            const schema = Schema.object({result: Schema.string()});
            const params = {value: 'hello'};
            const {result} = renderHook(() =>
                useQueryOne(
                    `MAP {result: $value}`,
                    params,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });


            expect(result.current.result!.rows[0]).toEqual({result: 'hello'});
        });

        it('should re-execute when query changes', async () => {
            const {result, rerender} = renderHook(
                ({query}) => useQueryOne(query, undefined, Schema.object({num: Schema.number()})),
                {initialProps: {query: `MAP {num: 1}`}}
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({num: 1});

            // Change query
            rerender({query: `MAP {num: 2}`});

            await waitFor(() => {
                expect(result.current.result!.rows[0]).toEqual({num: 2});
            });
        });

        it('should re-execute when params change', async () => {
            const schema = Schema.object({result: Schema.number()});
            const {result, rerender} = renderHook(
                ({params}) => useQueryOne(`MAP {result: $value}`, params, schema),
                {initialProps: {params: {value: 10}}}
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({result: 10});

            // Change params
            rerender({params: {value: 20}});

            await waitFor(() => {
                expect(result.current.result!.rows[0]).toEqual({result: 20});
            });
        });

        it('should handle schema conversion', async () => {
            const schema = Schema.object({
                name: Schema.string(),
                age: Schema.number()
            });

            const {result} = renderHook(() =>
                useQueryOne(
                    `MAP {name: 'Alice', age: 30}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            const person = result.current.result!.rows[0];
            expect(person).toEqual({name: 'Alice', age: 30});
        });

        it('should handle errors', async () => {
            const {result} = renderHook(() =>
                useQueryOne('INVALID QUERY SYNTAX',
                    undefined,
                    Schema.object({nothing: Schema.boolean()})
                ),
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.result).toBeUndefined();
        });

        it('should handle empty results', async () => {
            const {result} = renderHook(() =>
                useQueryOne(`FROM [{x:1}] FILTER x > 10`, undefined, Schema.object({x: Schema.number()}))
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.error).toBeUndefined();
            expect(result.current.result!.rows).toHaveLength(0);
        });
    });

    describe('useQueryMany', () => {
        it('should execute multiple queries', async () => {
            const schemas = [
                Schema.object({first: Schema.number()}),
                Schema.object({second: Schema.number()}),
                Schema.object({third: Schema.number()})
            ] as const;
            const queries = [
                `MAP {first: 1}`,
                `MAP {second: 2}`,
                `MAP {third: 3}`
            ];

            const {result} = renderHook(() =>
                useQueryMany(queries, undefined, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.results).toBeDefined();
            });

            expect(result.current.results).toHaveLength(3);
            expect(result.current.results![0].rows[0]).toEqual({first: 1});
            expect(result.current.results![1].rows[0]).toEqual({second: 2});
            expect(result.current.results![2].rows[0]).toEqual({third: 3});
        });

        it('should handle single statement as string', async () => {
            const {result} = renderHook(() =>
                useQueryMany(
                    `MAP {answer: 42}`,
                    undefined,
                    [Schema.object({answer: Schema.number()})]
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results).toHaveLength(1);
            expect(result.current.results![0].rows[0]).toEqual({answer: 42});
        });

        it('should handle parameters for multiple queries', async () => {
            const schemas = [
                Schema.object({first: Schema.number()}),
                Schema.object({second: Schema.number()})
            ] as const;
            const queries = [
                `MAP {first: $x}`,
                `MAP {second: $y}`
            ];
            const params = {x: 10, y: 20};

            const {result} = renderHook(() =>
                useQueryMany(queries, params, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results![0].rows[0]).toEqual({first: 10});
            expect(result.current.results![1].rows[0]).toEqual({second: 20});
        });

        it('should handle multiple schemas', async () => {
            const schemas = [
                Schema.object({value: Schema.number()}),
                Schema.object({name: Schema.string()})
            ] as const;

            const queries = [
                `MAP {value: 100}`,
                `MAP {name: 'test'}`
            ];

            const {result} = renderHook(() =>
                useQueryMany(queries, undefined, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results![0].rows[0]).toEqual({value: 100});
            expect(result.current.results![1].rows[0]).toEqual({name: 'test'});
        });

        it('should re-execute when statements change', async () => {
            const {result, rerender} = renderHook(
                ({queries}) => useQueryMany(queries, undefined, [Schema.object({x: Schema.number()})]),
                {initialProps: {queries: [`MAP {x: 1}`]}}
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results).toHaveLength(1);

            // Change queries
            rerender({queries: [`MAP {x: 1}`, `MAP {y: 2}`]});

            await waitFor(() => {
                expect(result.current.results).toHaveLength(2);
            });
        });

        it('should handle mixed success and empty results', async () => {
            const queries = [
                `MAP {value: 1}`,
                `FROM [{x:1}] FILTER x > 10`,
                `MAP {value: 2}`
            ];
            const schemas = [
                Schema.object({value: Schema.number()}),
                Schema.object({value: Schema.number()}),
                Schema.object({value: Schema.number()}),
            ];
            const {result} = renderHook(() =>
                useQueryMany(queries, undefined, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results).toHaveLength(3);
            expect(result.current.results![0].rows).toHaveLength(1);
            expect(result.current.results![1].rows).toHaveLength(0);  // Empty
            expect(result.current.results![2].rows).toHaveLength(1);
        });

        it('should handle errors in one of multiple queries', async () => {
            const queries = [
                `MAP {valid: 1}`,
                'INVALID SYNTAX',
                `MAP {valid: 2}`
            ];
            const schemas = [
                Schema.object({valid: Schema.number()}),
                Schema.object({valid: Schema.number()}),
                Schema.object({valid: Schema.number()}),
            ];
            const {result} = renderHook(() =>
                useQueryMany(queries, undefined, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // When one query fails, the entire batch fails
            expect(result.current.error).toBeDefined();
            expect(result.current.results).toBeUndefined();
        });
    });

    describe('Hook interaction', () => {
        it('should allow multiple hooks to run different queries simultaneously', async () => {
            const schema1 = Schema.object({value: Schema.number()});
            const {result: result1} = renderHook(() =>
                useQueryOne(`MAP {value: 100}`, undefined, schema1)
            );

            const queries2 = [`MAP {x: 200}`, `MAP {y: 300}`];
            const schemas2 = [
                Schema.object({x: Schema.number()}),
                Schema.object({y: Schema.number()})
            ] as const;
            const {result: result2} = renderHook(() =>
                useQueryMany(queries2, undefined, schemas2)
            );

            await waitFor(() => {
                expect(result1.current.isExecuting).toBe(false);
                expect(result2.current.isExecuting).toBe(false);
            });

            expect(result1.current.result!.rows[0]).toEqual({value: 100});
            expect(result2.current.results![0].rows[0]).toEqual({x: 200});
            expect(result2.current.results![1].rows[0]).toEqual({y: 300});
        });
    });
});