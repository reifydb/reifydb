// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useQueryOne, useQueryMany, getConnection, clearConnection, Shape} from '../../../src';
import {waitForDatabase} from '../setup';

describe('useQuery with TypeScript Primitive Types', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection({token: process.env.REIFYDB_TOKEN});
        await conn.connect();
    }, 30000);

    afterAll(() => {
        clearConnection();
    });

    describe('Primitive Type - With Shape Returns JS Primitives', () => {
        describe('String Type', () => {
            it('should handle string primitive type', async () => {
                const shape = Shape.object({ name: Shape.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {name: 'John Doe'}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With shape, strings return as JS primitives
                expect(result.current.result!.rows[0].name).toBe('John Doe');
                expect(typeof result.current.result!.rows[0].name).toBe('string');
            });

            it('should handle string with special characters', async () => {
                const shape = Shape.object({ text: Shape.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {text: 'Hello World'}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With shape, strings return as JS primitives
                expect(result.current.error).toBeUndefined();
                expect(result.current.result).toBeDefined();
                expect(result.current.result!.rows[0].text).toBe('Hello World');
            });

            it('should handle empty string', async () => {
                const shape = Shape.object({ empty: Shape.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {empty: ''}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With shape, strings return as JS primitives
                expect(result.current.result!.rows[0].empty).toBe('');
            });
        });

        describe('Number Types', () => {
            it('should handle number primitive type', async () => {
                const shape = Shape.object({ age: Shape.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {age: 25}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].age).toBe(25);
                expect(typeof result.current.result!.rows[0].age).toBe('number');
            });

            it('should handle float numbers', async () => {
                const shape = Shape.object({ price: Shape.float() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {price: 19.99}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].price).toBe(19.99);
            });

            it('should handle double precision numbers', async () => {
                const shape = Shape.object({ value: Shape.double() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {value: 3.141592653589793}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value).toBeCloseTo(3.141592653589793);
            });

            it('should handle decimal numbers', async () => {
                const shape = Shape.object({ amount: Shape.decimal() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {amount: cast('123.456789', decimal)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].amount).toBe('123.456789');
                expect(typeof result.current.result!.rows[0].amount).toBe('string');
            });

            it('should handle integer type', async () => {
                const shape = Shape.object({ count: Shape.int() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {count: 100}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].count).toBe(100);
                expect(Number.isInteger(result.current.result!.rows[0].count)).toBe(true);
            });

            it('should handle negative numbers', async () => {
                const shape = Shape.object({ temperature: Shape.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {temperature: -40.5}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].temperature).toBe(-40.5);
            });

            it('should handle zero', async () => {
                const shape = Shape.object({ zero: Shape.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {zero: 0}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].zero).toBe(0);
            });
        });

        describe('Boolean Type', () => {
            it('should handle boolean true', async () => {
                const shape = Shape.object({ active: Shape.boolean() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {active: true}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].active).toBe(true);
                expect(typeof result.current.result!.rows[0].active).toBe('boolean');
            });

            it('should handle boolean false', async () => {
                const shape = Shape.object({ enabled: Shape.bool() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {enabled: false}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].enabled).toBe(false);
            });
        });

        describe('Special Types', () => {
            it('should handle undefined type', async () => {
                const shape = Shape.object({ missing: Shape.optional(Shape.string()) });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {missing: undefined}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].missing).toBeUndefined();
            });

        });
    });

    describe('Complex Shape Scenarios', () => {
        it('should handle string shape', async () => {
            const shape = Shape.object({
                name: Shape.string()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {name: 'Alice'}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // With shape, strings are still JS strings
            expect(result.current.result!.rows[0].name).toBe('Alice');
            expect(typeof result.current.result!.rows[0].name).toBe('string');
        });

        it('should handle number shape', async () => {
            const shape = Shape.object({
                age: Shape.number()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {age: 30}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // With shape, numbers are still JS numbers  
            expect(result.current.result!.rows[0].age).toBe(30);
            expect(typeof result.current.result!.rows[0].age).toBe('number');
        });

        it('should handle boolean shape', async () => {
            const shape = Shape.object({
                active: Shape.boolean()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {active: true}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0].active).toBe(true);
            expect(typeof result.current.result!.rows[0].active).toBe('boolean');
        });

        it('should handle optional fields', async () => {
            const shape = Shape.object({
                required: Shape.string(),
                optional: Shape.optional(Shape.number())
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {required: 'present', optional: undefined}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0].required).toBe('present');
            expect(result.current.result!.rows[0].optional).toBeUndefined();
        });
    });

    describe('Primitive Type Parameters', () => {
        it('should handle primitive parameters', async () => {
            const params = { name: 'Parameter Value' };
            const shape = Shape.object({ result: Shape.string() });
            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {result: $name}`,
                    params,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0].result).toBe('Parameter Value');
        });

        it('should handle multiple primitive parameters', async () => {
            const params = { 
                a: 10, 
                b: 20, 
                prefix: 'Hello, ', 
                suffix: 'World!',
                isActive: true 
            };
            const shape = Shape.object({
                sum: Shape.number(),
                concat: Shape.string(),
                flag: Shape.boolean()
            });
            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {
                        sum: $a + $b,
                        concat: $prefix + $suffix,
                        flag: $isActive
                    }`,
                    params,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            const data = result.current.result!.rows[0];
            expect(data.sum).toBe(30);
            expect(data.concat).toBe('Hello, World!');
            expect(data.flag).toBe(true);
        });
    });

    describe('useQueryMany with mixed shapes', () => {
        it('should handle multiple queries without shapes', async () => {
            const queries = [
                `MAP {str: 'test'}`,
                `MAP {num: 42}`,
                `MAP {bool: true}`
            ];

            const { result } = renderHook(() =>
                useQueryMany(queries)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // Without shapes, these return value objects
            // @ts-ignore
            expect(result.current.results![0].rows[0].str.type).toBe('Utf8');
            expect(result.current.results![1].rows[0].num.type).toBe('Int1');
            expect(result.current.results![2].rows[0].bool.type).toBe('Boolean');
        });

        it('should handle multiple queries with shapes', async () => {
            const shapes = [
                Shape.object({ value: Shape.string() }),
                Shape.object({ value: Shape.number() })
            ] as const;
            const queries = [
                `MAP {value: 'hello'}`,
                `MAP {value: 123}`
            ];

            const { result } = renderHook(() => 
                useQueryMany(queries, undefined, shapes)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results![0].rows[0].value).toBe('hello');
            expect(result.current.results![1].rows[0].value).toBe(123);
        });
    });
});