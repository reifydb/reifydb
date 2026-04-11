// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {afterAll, beforeAll, afterEach, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useCommandOne, useCommandMany, ConnectionProvider, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database} from '../setup';
// @ts-ignore
import React from "react";

describe('useCommand Hooks', () => {
    // Wrapper to provide ConnectionProvider to all hooks
    const wrapper = ({children}: { children: React.ReactNode }) => (
        <ConnectionProvider config={{url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN}} children={children}/>
    );

    beforeAll(async () => {
        await wait_for_database();
    }, 30000);

    afterEach(async () => {
        // Clear all connections after each test to prevent interference
        await clear_connection();
    });

    afterAll(async () => {
        await clear_connection();
    });

    describe('useCommandOne', () => {
        it('should execute a single command and return one result', async () => {
            const shape = Shape.object({answer: Shape.number()});
            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {answer: 42}`,
                    undefined,
                    shape
                ), {wrapper}
            );

            expect(result.current.is_executing).toBe(true);
            expect(result.current.result).toBeUndefined();

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.result).toBeDefined();
            });

            expect(result.current.error).toBeUndefined();
            expect(result.current.result!.rows).toHaveLength(1);
            expect(result.current.result!.rows[0]).toEqual({answer: 42});
        });

        it('should handle parameters', async () => {
            const shape = Shape.object({result: Shape.string()});
            const params = {value: 'hello'};
            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {result: $value}`,
                    params,
                    shape
                ), {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });


            expect(result.current.result!.rows[0]).toEqual({result: 'hello'});
        });

        it('should re-execute when command changes', async () => {
            const {result, rerender} = renderHook(
                ({command}) => useCommandOne(command, undefined, Shape.object({num: Shape.number()})),
                {initialProps: {command: `MAP {num: 1}`}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({num: 1});

            // Change command
            rerender({command: `MAP {num: 2}`});

            await waitFor(() => {
                expect(result.current.result!.rows[0]).toEqual({num: 2});
            });
        });

        it('should re-execute when params change', async () => {
            const shape = Shape.object({result: Shape.number()});
            const {result, rerender} = renderHook(
                ({params}) => useCommandOne(`MAP {result: $value}`, params, shape),
                {initialProps: {params: {value: 10}}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({result: 10});

            // Change params
            rerender({params: {value: 20}});

            await waitFor(() => {
                expect(result.current.result!.rows[0]).toEqual({result: 20});
            });
        });

        it('should handle shape conversion', async () => {
            const shape = Shape.object({
                name: Shape.string(),
                age: Shape.number()
            });

            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {name: 'Alice', age: 30}`,
                    undefined,
                    shape
                ), {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            const person = result.current.result!.rows[0];
            expect(person).toEqual({name: 'Alice', age: 30});
        });

        it('should handle errors', async () => {
            const {result} = renderHook(() =>
                useCommandOne('INVALID COMMAND SYNTAX',
                    undefined,
                    Shape.object({nothing: Shape.boolean()})
                ), {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.result).toBeUndefined();
        });

        it('should handle empty results', async () => {
            const {result} = renderHook(() =>
                useCommandOne(`FROM [{x:1}] FILTER x > 10`, undefined, Shape.object({x: Shape.number()}))
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.error).toBeUndefined();
            expect(result.current.result!.rows).toHaveLength(0);
        });
    });

    describe('useCommandMany', () => {
        it('should execute multiple queries', async () => {
            const shapes = [
                Shape.object({first: Shape.number()}),
                Shape.object({second: Shape.number()}),
                Shape.object({third: Shape.number()})
            ] as const;
            const queries = [
                `MAP {first: 1}`,
                `MAP {second: 2}`,
                `MAP {third: 3}`
            ];

            const {result} = renderHook(() =>
                useCommandMany(queries, undefined, shapes)
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.results).toBeDefined();
            });

            expect(result.current.results).toHaveLength(3);
            expect(result.current.results![0].rows[0]).toEqual({first: 1});
            expect(result.current.results![1].rows[0]).toEqual({second: 2});
            expect(result.current.results![2].rows[0]).toEqual({third: 3});
        });

        it('should handle single statement as string', async () => {
            const {result} = renderHook(() =>
                useCommandMany(
                    `MAP {answer: 42}`,
                    undefined,
                    [Shape.object({answer: Shape.number()})]
                )
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.results).toHaveLength(1);
            expect(result.current.results![0].rows[0]).toEqual({answer: 42});
        });

        it('should handle parameters for multiple queries', async () => {
            const shapes = [
                Shape.object({first: Shape.number()}),
                Shape.object({second: Shape.number()})
            ] as const;
            const queries = [
                `MAP {first: $x}`,
                `MAP {second: $y}`
            ];
            const params = {x: 10, y: 20};

            const {result} = renderHook(() =>
                useCommandMany(queries, params, shapes)
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.results![0].rows[0]).toEqual({first: 10});
            expect(result.current.results![1].rows[0]).toEqual({second: 20});
        });

        it('should handle multiple shapes', async () => {
            const shapes = [
                Shape.object({value: Shape.number()}),
                Shape.object({name: Shape.string()})
            ] as const;

            const queries = [
                `MAP {value: 100}`,
                `MAP {name: 'test'}`
            ];

            const {result} = renderHook(() =>
                useCommandMany(queries, undefined, shapes)
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.results![0].rows[0]).toEqual({value: 100});
            expect(result.current.results![1].rows[0]).toEqual({name: 'test'});
        });

        it('should re-execute when statements change', async () => {
            const {result, rerender} = renderHook(
                ({queries}) => useCommandMany(queries, undefined, [Shape.object({x: Shape.number()})]),
                {initialProps: {queries: [`MAP {x: 1}`]}, wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
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
            const shapes = [
                Shape.object({value: Shape.number()}),
                Shape.object({value: Shape.number()}),
                Shape.object({value: Shape.number()}),
            ];
            const {result} = renderHook(() =>
                useCommandMany(queries, undefined, shapes)
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
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
            const shapes = [
                Shape.object({valid: Shape.number()}),
                Shape.object({valid: Shape.number()}),
                Shape.object({valid: Shape.number()}),
            ];
            const {result} = renderHook(() =>
                useCommandMany(queries, undefined, shapes)
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            // When one command fails, the entire batch fails
            expect(result.current.error).toBeDefined();
            expect(result.current.results).toBeUndefined();
        });
    });

    describe('Hook interaction', () => {
        it('should allow multiple hooks to run different queries simultaneously', async () => {
            const shape1 = Shape.object({value: Shape.number()});
            const {result: result1} = renderHook(() =>
                useCommandOne(`MAP {value: 100}`, undefined, shape1)
            , {wrapper});

            const queries2 = [`MAP {x: 200}`, `MAP {y: 300}`];
            const shapes2 = [
                Shape.object({x: Shape.number()}),
                Shape.object({y: Shape.number()})
            ] as const;
            const {result: result2} = renderHook(() =>
                useCommandMany(queries2, undefined, shapes2)
            , {wrapper});

            await waitFor(() => {
                expect(result1.current.is_executing).toBe(false);
                expect(result2.current.is_executing).toBe(false);
            });

            expect(result1.current.result!.rows[0]).toEqual({value: 100});
            expect(result2.current.results![0].rows[0]).toEqual({x: 200});
            expect(result2.current.results![1].rows[0]).toEqual({y: 300});
        });

        it('should work with ConnectionProvider', async () => {
            // @ts-ignore
            const wrapper = ({children}: { children: React.ReactNode }) => (
                <ConnectionProvider config={{url: process.env.REIFYDB_WS_URL!, token: process.env.REIFYDB_TOKEN}} children={children}/>
            );

            const shape = Shape.object({value: Shape.number()});
            const {result} = renderHook(
                () => useCommandOne(`MAP {value: 999}`, undefined, shape),
                {wrapper}
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({value: 999});
        });

        it('should support config override in hooks', async () => {
            const shape = Shape.object({test: Shape.string()});
            const override_config = {url: process.env.REIFYDB_WS_URL!, options: {timeout_ms: 2000}};

            // Use override config (different timeout to ensure it's treated as a separate connection)
            const {result, unmount} = renderHook(() =>
                useCommandOne(
                    `MAP {test: 'override'}`,
                    undefined,
                    shape,
                    {connection_config: override_config}
                )
            , {wrapper});

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0]).toEqual({test: 'override'});
            
            // Clean up the override connection
            unmount();
            await clear_connection(override_config);
        });
    });
});