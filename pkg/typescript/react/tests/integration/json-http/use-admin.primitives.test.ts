// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useAdminOne, useAdminMany, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database_http} from '../setup';

describe('useAdmin with TypeScript Primitive Types (JSON HTTP)', () => {
    beforeAll(async () => {
        await wait_for_database_http();
        const conn = get_connection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN, format: 'json'});
        await conn.connect();
    }, 30000);

    afterAll(() => {
        clear_connection();
    });

    describe('Primitive Type - With Shape Returns JS Primitives', () => {
        describe('String Type', () => {
            it('should handle string primitive type', async () => {
                const shape = Shape.object({ name: Shape.string() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {name: 'John Doe'}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].name).toBe('John Doe');
                expect(typeof result.current.result!.rows[0].name).toBe('string');
            });

            it('should handle string with special characters', async () => {
                const shape = Shape.object({ text: Shape.string() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {text: 'Hello World'}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.error).toBeUndefined();
                expect(result.current.result).toBeDefined();
                expect(result.current.result!.rows[0].text).toBe('Hello World');
            });

            it('should handle empty string', async () => {
                const shape = Shape.object({ empty: Shape.string() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {empty: ''}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].empty).toBe('');
            });
        });

        describe('Number Types', () => {
            it('should handle number primitive type', async () => {
                const shape = Shape.object({ age: Shape.number() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {age: 25}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].age).toBe(25);
                expect(typeof result.current.result!.rows[0].age).toBe('number');
            });

            it('should handle float numbers', async () => {
                const shape = Shape.object({ price: Shape.float() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {price: 19.99}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].price).toBe(19.99);
            });

            it('should handle double precision numbers', async () => {
                const shape = Shape.object({ value: Shape.double() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {value: 3.141592653589793}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].value).toBeCloseTo(3.141592653589793);
            });

            it('should handle decimal numbers', async () => {
                const shape = Shape.object({ amount: Shape.decimal() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {amount: cast('123.456789', decimal)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].amount).toBe('123.456789');
                expect(typeof result.current.result!.rows[0].amount).toBe('string');
            });

            it('should handle integer type', async () => {
                const shape = Shape.object({ count: Shape.int() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {count: 100}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].count).toBe(100);
                expect(Number.isInteger(result.current.result!.rows[0].count)).toBe(true);
            });

            it('should handle negative numbers', async () => {
                const shape = Shape.object({ temperature: Shape.number() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {temperature: -40.5}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].temperature).toBe(-40.5);
            });

            it('should handle zero', async () => {
                const shape = Shape.object({ zero: Shape.number() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {zero: 0}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].zero).toBe(0);
            });
        });

        describe('Boolean Type', () => {
            it('should handle boolean true', async () => {
                const shape = Shape.object({ active: Shape.boolean() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {active: true}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].active).toBe(true);
                expect(typeof result.current.result!.rows[0].active).toBe('boolean');
            });

            it('should handle boolean false', async () => {
                const shape = Shape.object({ enabled: Shape.bool() });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {enabled: false}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].enabled).toBe(false);
            });
        });

        describe('Special Types', () => {
            it('should handle undefined type', async () => {
                const shape = Shape.object({ missing: Shape.optional(Shape.string()) });
                const { result } = renderHook(() =>
                    useAdminOne(
                        `MAP {missing: undefined}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].missing).toBeNull();
            });

        });
    });

    describe('Complex Shape Scenarios', () => {
        it('should handle string shape', async () => {
            const shape = Shape.object({
                name: Shape.string()
            });

            const { result } = renderHook(() =>
                useAdminOne(
                    `MAP {name: 'Alice'}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0].name).toBe('Alice');
            expect(typeof result.current.result!.rows[0].name).toBe('string');
        });

        it('should handle number shape', async () => {
            const shape = Shape.object({
                age: Shape.number()
            });

            const { result } = renderHook(() =>
                useAdminOne(
                    `MAP {age: 30}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0].age).toBe(30);
            expect(typeof result.current.result!.rows[0].age).toBe('number');
        });

        it('should handle boolean shape', async () => {
            const shape = Shape.object({
                active: Shape.boolean()
            });

            const { result } = renderHook(() =>
                useAdminOne(
                    `MAP {active: true}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
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
                useAdminOne(
                    `MAP {required: 'present', optional: undefined}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.result!.rows[0].required).toBe('present');
            expect(result.current.result!.rows[0].optional).toBeNull();
        });
    });

    describe('Primitive Type Parameters', () => {
        it('should handle primitive parameters', async () => {
            const params = { name: 'Parameter Value' };
            const shape = Shape.object({ result: Shape.string() });
            const { result } = renderHook(() =>
                useAdminOne(
                    `MAP {result: $name}`,
                    params,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
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
                useAdminOne(
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
                expect(result.current.is_executing).toBe(false);
            });

            const data = result.current.result!.rows[0];
            expect(data.sum).toBe(30);
            expect(data.concat).toBe('Hello, World!');
            expect(data.flag).toBe(true);
        });
    });

    describe('useAdminMany with mixed shapes', () => {
        it('should handle multiple queries without shapes', async () => {
            const queries = `OUTPUT MAP {str: 'test'}; OUTPUT MAP {num: 42}; OUTPUT MAP {bool: true}`;

            const { result } = renderHook(() =>
                useAdminMany(queries)
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.results![0].rows[0].str).toBe('test');
            expect(typeof result.current.results![0].rows[0].str).toBe('string');
            expect(result.current.results![1].rows[0].num).toBe(42);
            expect(typeof result.current.results![1].rows[0].num).toBe('number');
            expect(result.current.results![2].rows[0].bool).toBe(true);
            expect(typeof result.current.results![2].rows[0].bool).toBe('boolean');
        });

        it('should handle multiple queries with shapes', async () => {
            const shapes = [
                Shape.object({ value: Shape.string() }),
                Shape.object({ value: Shape.number() })
            ] as const;
            const queries = `OUTPUT MAP {value: 'hello'}; OUTPUT MAP {value: 123}`;

            const { result } = renderHook(() =>
                useAdminMany(queries, undefined, shapes)
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
            });

            expect(result.current.results![0].rows[0].value).toBe('hello');
            expect(result.current.results![1].rows[0].value).toBe(123);
        });
    });
});
