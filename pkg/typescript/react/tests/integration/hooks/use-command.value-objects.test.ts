/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useCommandOne, useCommandMany, getConnection, clearAllConnections, Schema} from '../../../src';
import {waitForDatabase} from '../setup';

describe('useCommand with Value Objects and Schemas', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection();
        await conn.connect();
    }, 30000);

    afterAll(() => {
        clearAllConnections();
    });

    describe('Value Objects', () => {
        describe('Integer Types', () => {
            it('should handle Int1 value objects', async () => {
                const schema = Schema.object({
                    value: Schema.int1Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(127, int1)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.error).toBeUndefined();
                expect(result.current.result!.rows[0].value).toBeDefined();
                expect(result.current.result!.rows[0].value.type).toBe('Int1');
            });

            it('should handle Int1 range error', async () => {
                const schema = Schema.object({
                    value: Schema.int1Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(129, int1)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                    expect(result.current.error).toBeDefined();
                });

                expect(result.current.error).toContain('CAST_002');
            });

            it('should handle Int4 value objects', async () => {
                const schema = Schema.object({
                    num: Schema.int4Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {num: cast(2147483647, int4)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].num.type).toBe('Int4');
            });

            it('should handle Int8 value objects', async () => {
                const schema = Schema.object({
                    bigNum: Schema.int8Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {bigNum: cast(9223372036854775807, int8)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].bigNum.type).toBe('Int8');
            });
        });

        describe('Unsigned Integer Types', () => {
            it('should handle Uint1 value objects', async () => {
                const schema = Schema.object({
                    value: Schema.uint1Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(255, uint1)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Uint1');
            });

            it('should handle Uint4 value objects', async () => {
                const schema = Schema.object({
                    value: Schema.uint4Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(4294967295, uint4)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Uint4');
            });
        });

        describe('Float Types', () => {
            it('should handle Float4 value objects', async () => {
                const schema = Schema.object({
                    value: Schema.float4Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(3.14159, float4)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Float4');
            });

            it('should handle Float8 value objects', async () => {
                const schema = Schema.object({
                    value: Schema.float8Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {value: cast(3.141592653589793, float8)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Float8');
            });
        });

        describe('String and Binary Types', () => {
            it('should handle Utf8 value objects', async () => {
                const schema = Schema.object({
                    text: Schema.utf8Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {text: cast('Hello, World!', utf8)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].text.type).toBe('Utf8');
            });

            it('should handle Blob value objects - unsupported cast from Utf8 to Blob', async () => {
                const schema = Schema.object({
                    data: Schema.blobValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {data: cast('binary data', blob)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result).toBeUndefined();
                expect(result.current.error).toBeDefined();
            });
        });

        describe('Date and Time Types', () => {
            it('should handle Date value objects', async () => {
                const schema = Schema.object({
                    date: Schema.dateValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {date: cast('2025-01-09', date)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].date.type).toBe('Date');
            });

            it('should handle DateTime value objects', async () => {
                const schema = Schema.object({
                    timestamp: Schema.dateTimeValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {timestamp: cast('2025-01-09T12:00:00Z', datetime)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].timestamp.type).toBe('DateTime');
            });

            it('should handle Time value objects', async () => {
                const schema = Schema.object({
                    time: Schema.timeValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {time: cast('12:30:45', time)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].time.type).toBe('Time');
            });

            it('should handle Interval value objects', async () => {
                const schema = Schema.object({
                    duration: Schema.intervalValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {duration: cast('PT1H30M', interval)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].duration.type).toBe('Interval');
            });
        });

        describe('UUID Types', () => {
            it('should handle Uuid4 value objects', async () => {
                const schema = Schema.object({
                    id: Schema.uuid4Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {id: cast('550e8400-e29b-41d4-a716-446655440000', uuid4)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].id.type).toBe('Uuid4');
            });

            it('should handle Uuid7 value objects', async () => {
                const schema = Schema.object({
                    id: Schema.uuid7Value()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {id: cast('018a4d65-4307-7834-8336-a5c62b3c1234', uuid7)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].id.type).toBe('Uuid7');
            });
        });

        describe('Special Types', () => {
            it('should handle Boolean value objects', async () => {
                const schema = Schema.object({
                    flag: Schema.booleanValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {flag: cast(true, boolean)}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].flag.type).toBe('Boolean');
            });

            it('should handle Undefined value objects', async () => {
                const schema = Schema.object({
                    nothing: Schema.undefinedValue()
                });

                const {result} = renderHook(() =>
                    useCommandOne(
                        `MAP {nothing: undefined}`,
                        undefined,
                        schema
                    )
                );

                await waitFor(() => {
                    expect(result.current.isExecuting).toBe(false);
                });

                expect(result.current.result!.rows[0].nothing.type).toBe('Undefined');
            });
        });
    });

    describe('Error Handling for Value Objects', () => {
        it('should handle type conversion errors ', async () => {
            const schema = Schema.object({
                value: Schema.int4Value()
            });

            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {value: cast('not a number', int4)}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.result).toBeUndefined();
        });

        it('should handle overflow errors for numeric types ', async () => {
            const schema = Schema.object({
                value: Schema.int2Value()
            });

            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {value: cast(32768, int2)}`, // Max int2 is 32767
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.error).toContain('CAST_002');
        });

        it('should handle invalid UUID format', async () => {
            const schema = Schema.object({
                id: Schema.uuid4Value()
            });

            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {id: cast('invalid-uuid', uuid4)}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.error).toBeDefined();
            });
        });

        it('should handle invalid date format ', async () => {
            const schema = Schema.object({
                date: Schema.dateValue()
            });

            const {result} = renderHook(() =>
                useCommandOne(
                    `MAP {date: cast('not-a-date', date)}`,
                    undefined,
                    schema
                )
            );

            await waitFor(() => {
                expect(result.current.isExecuting).toBe(false);
                expect(result.current.error).toBeDefined();
            });
        });
    });
});