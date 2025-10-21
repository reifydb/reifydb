/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterEach, afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useQueryOne, useQueryMany, getConnection, clearAllConnections, Schema} from '../../../src';
import {waitForDatabase} from '../setup';

describe('useQuery with TypeScript Primitive Types', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection();
        await conn.connect();
    }, 30000);

    afterAll(() => {
        clearAllConnections();
    });

    describe('Primitive Type - With Schema Returns JS Primitives', () => {
        describe('String Type', () => {
            it('should handle string primitive type', async () => {
                const schema = Schema.object({ name: Schema.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {name: 'John Doe'}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With schema, strings return as JS primitives
                expect(result.current.result!.rows[0].name).toBe('John Doe');
                expect(typeof result.current.result!.rows[0].name).toBe('string');
            });

            it('should handle string with special characters', async () => {
                const schema = Schema.object({ text: Schema.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {text: 'Hello World'}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With schema, strings return as JS primitives
                expect(result.current.error).toBeUndefined();
                expect(result.current.result).toBeDefined();
                expect(result.current.result!.rows[0].text).toBe('Hello World');
            });

            it('should handle empty string', async () => {
                const schema = Schema.object({ empty: Schema.string() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {empty: ''}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                // With schema, strings return as JS primitives
                expect(result.current.result!.rows[0].empty).toBe('');
            });
        });

        describe('Number Types', () => {
            it('should handle number primitive type', async () => {
                const schema = Schema.object({ age: Schema.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {age: 25}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].age).toBe(25);
                expect(typeof result.current.result!.rows[0].age).toBe('number');
            });

            it('should handle float numbers', async () => {
                const schema = Schema.object({ price: Schema.float() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {price: 19.99}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].price).toBe(19.99);
            });

            it('should handle double precision numbers', async () => {
                const schema = Schema.object({ value: Schema.double() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {value: 3.141592653589793}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value).toBeCloseTo(3.141592653589793);
            });

            it('should handle decimal numbers', async () => {
                const schema = Schema.object({ amount: Schema.decimal() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {amount: cast('123.456789', decimal)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].amount).toBe('123.456789');
                expect(typeof result.current.result!.rows[0].amount).toBe('string');
            });

            it('should handle integer type', async () => {
                const schema = Schema.object({ count: Schema.int() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {count: 100}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].count).toBe(100);
                expect(Number.isInteger(result.current.result!.rows[0].count)).toBe(true);
            });

            it('should handle negative numbers', async () => {
                const schema = Schema.object({ temperature: Schema.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {temperature: -40.5}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].temperature).toBe(-40.5);
            });

            it('should handle zero', async () => {
                const schema = Schema.object({ zero: Schema.number() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {zero: 0}`,
                        undefined,
                        schema
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
                const schema = Schema.object({ active: Schema.boolean() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {active: true}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].active).toBe(true);
                expect(typeof result.current.result!.rows[0].active).toBe('boolean');
            });

            it('should handle boolean false', async () => {
                const schema = Schema.object({ enabled: Schema.bool() });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {enabled: false}`,
                        undefined,
                        schema
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
                const schema = Schema.object({ missing: Schema.optional(Schema.string()) });
                const { result } = renderHook(() => 
                    useQueryOne(
                        `MAP {missing: undefined}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].missing).toBeUndefined();
            });

        });
    });

    describe('Complex Schema Scenarios', () => {
        it('should handle string schema', async () => {
            const schema = Schema.object({
                name: Schema.string()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {name: 'Alice'}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // With schema, strings are still JS strings
            expect(result.current.result!.rows[0].name).toBe('Alice');
            expect(typeof result.current.result!.rows[0].name).toBe('string');
        });

        it('should handle number schema', async () => {
            const schema = Schema.object({
                age: Schema.number()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {age: 30}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            // With schema, numbers are still JS numbers  
            expect(result.current.result!.rows[0].age).toBe(30);
            expect(typeof result.current.result!.rows[0].age).toBe('number');
        });

        it('should handle boolean schema', async () => {
            const schema = Schema.object({
                active: Schema.boolean()
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {active: true}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.result!.rows[0].active).toBe(true);
            expect(typeof result.current.result!.rows[0].active).toBe('boolean');
        });

        it('should handle optional fields', async () => {
            const schema = Schema.object({
                required: Schema.string(),
                optional: Schema.optional(Schema.number())
            });

            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {required: 'present', optional: undefined}`,
                    undefined,
                    schema
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
            const schema = Schema.object({ result: Schema.string() });
            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {result: $name}`,
                    params,
                    schema
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
            const schema = Schema.object({
                sum: Schema.number(),
                concat: Schema.string(),
                flag: Schema.boolean()
            });
            const { result } = renderHook(() => 
                useQueryOne(
                    `MAP {
                        sum: $a + $b,
                        concat: $prefix + $suffix,
                        flag: $isActive
                    }`,
                    params,
                    schema
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

    describe('useQueryMany with mixed schemas', () => {
        it('should handle multiple queries without schemas', async () => {
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

            // Without schemas, these return value objects
            // @ts-ignore
            expect(result.current.results![0].rows[0].str.type).toBe('Utf8');
            expect(result.current.results![1].rows[0].num.type).toBe('Int1');
            expect(result.current.results![2].rows[0].bool.type).toBe('Boolean');
        });

        it('should handle multiple queries with schemas', async () => {
            const schemas = [
                Schema.object({ value: Schema.string() }),
                Schema.object({ value: Schema.number() })
            ] as const;
            const queries = [
                `MAP {value: 'hello'}`,
                `MAP {value: 123}`
            ];

            const { result } = renderHook(() => 
                useQueryMany(queries, undefined, schemas)
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
            });

            expect(result.current.results![0].rows[0].value).toBe('hello');
            expect(result.current.results![1].rows[0].value).toBe(123);
        });
    });
});