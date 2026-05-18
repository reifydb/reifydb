// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useAdminOne, useAdminMany, get_connection, clear_connection, Shape} from '../../../src';
import {wait_for_database_http} from '../setup';

describe('useAdmin with Value Objects and Shapes (HTTP)', () => {
    beforeAll(async () => {
        await wait_for_database_http();
        const conn = get_connection({url: process.env.REIFYDB_HTTP_URL, token: process.env.REIFYDB_TOKEN});
        await conn.connect();
    }, 30000);

    afterAll(() => {
        clear_connection();
    });

    describe('Value Objects', () => {
        describe('Integer Types', () => {
            it('should handle Int1 value objects', async () => {
                const shape = Shape.object({
                    value: Shape.int1Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(127, int1)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.error).toBeUndefined();
                expect(result.current.result!.rows[0].value).toBeDefined();
                expect(result.current.result!.rows[0].value.type).toBe('Int1');
            });

            it('should handle Int1 range error', async () => {
                const shape = Shape.object({
                    value: Shape.int1Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(129, int1)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                    expect(result.current.error).toBeDefined();
                });

                expect(result.current.error).toContain('CAST_002');
            });

            it('should handle Int4 value objects', async () => {
                const shape = Shape.object({
                    num: Shape.int4Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {num: cast(2147483647, int4)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].num.type).toBe('Int4');
            });

            it('should handle Int8 value objects', async () => {
                const shape = Shape.object({
                    bigNum: Shape.int8Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {bigNum: cast(9223372036854775807, int8)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].bigNum.type).toBe('Int8');
            });
        });

        describe('Unsigned Integer Types', () => {
            it('should handle Uint1 value objects', async () => {
                const shape = Shape.object({
                    value: Shape.uint1Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(255, uint1)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Uint1');
            });

            it('should handle Uint4 value objects', async () => {
                const shape = Shape.object({
                    value: Shape.uint4Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(4294967295, uint4)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Uint4');
            });
        });

        describe('Float Types', () => {
            it('should handle Float4 value objects', async () => {
                const shape = Shape.object({
                    value: Shape.float4Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(3.14159, float4)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Float4');
            });

            it('should handle Float8 value objects', async () => {
                const shape = Shape.object({
                    value: Shape.float8Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {value: cast(3.141592653589793, float8)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].value.type).toBe('Float8');
            });

            it('should handle Decimal value objects', async () => {
                const shape = Shape.object({
                    amount: Shape.decimalValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {amount: cast('123.456789', decimal)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.error).toBeUndefined();
                expect(result.current.result!.rows[0].amount).toBeDefined();
                expect(result.current.result!.rows[0].amount.type).toBe('Decimal');
                expect(result.current.result!.rows[0].amount.value).toBe('123.456789');
            });
        });

        describe('String and Binary Types', () => {
            it('should handle Utf8 value objects', async () => {
                const shape = Shape.object({
                    text: Shape.utf8Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {text: cast('Hello, World!', utf8)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].text.type).toBe('Utf8');
            });

            it('should handle Blob value objects - unsupported cast from Utf8 to Blob', async () => {
                const shape = Shape.object({
                    data: Shape.blobValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {data: cast('binary data', blob)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result).toBeUndefined();
                expect(result.current.error).toBeDefined();
            });
        });

        describe('Date and Time Types', () => {
            it('should handle Date value objects', async () => {
                const shape = Shape.object({
                    date: Shape.dateValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {date: cast('2025-01-09', date)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].date.type).toBe('Date');
            });

            it('should handle DateTime value objects', async () => {
                const shape = Shape.object({
                    timestamp: Shape.dateTimeValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {timestamp: cast('2025-01-09T12:00:00Z', datetime)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].timestamp.type).toBe('DateTime');
            });

            it('should handle Time value objects', async () => {
                const shape = Shape.object({
                    time: Shape.timeValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {time: cast('12:30:45', time)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].time.type).toBe('Time');
            });

            it('should handle Duration value objects', async () => {
                const shape = Shape.object({
                    duration: Shape.durationValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {duration: cast('PT1H30M', duration)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].duration.type).toBe('Duration');
            });
        });

        describe('UUID Types', () => {
            it('should handle Uuid4 value objects', async () => {
                const shape = Shape.object({
                    id: Shape.uuid4Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {id: cast('550e8400-e29b-41d4-a716-446655440000', uuid4)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].id.type).toBe('Uuid4');
            });

            it('should handle Uuid7 value objects', async () => {
                const shape = Shape.object({
                    id: Shape.uuid7Value()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {id: cast('018a4d65-4307-7834-8336-a5c62b3c1234', uuid7)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].id.type).toBe('Uuid7');
            });
        });

        describe('Special Types', () => {
            it('should handle Boolean value objects', async () => {
                const shape = Shape.object({
                    flag: Shape.booleanValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {flag: cast(true, boolean)}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].flag.type).toBe('Boolean');
            });

            it('should handle None value objects', async () => {
                const shape = Shape.object({
                    nothing: Shape.noneValue()
                });

                const {result} = renderHook(() =>
                    useAdminOne(
                        `MAP {nothing: undefined}`,
                        undefined,
                        shape
                    )
                );

                await waitFor(() => {
                    expect(result.current.is_executing).toBe(false);
                });

                expect(result.current.result!.rows[0].nothing.type).toBe('None');
            });
        });
    });

    describe('Error Handling for Value Objects', () => {
        it('should handle type conversion errors ', async () => {
            const shape = Shape.object({
                value: Shape.int4Value()
            });

            const {result} = renderHook(() =>
                useAdminOne(
                    `MAP {value: cast('not a number', int4)}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.result).toBeUndefined();
        });

        it('should handle overflow errors for numeric types ', async () => {
            const shape = Shape.object({
                value: Shape.int2Value()
            });

            const {result} = renderHook(() =>
                useAdminOne(
                    `MAP {value: cast(32768, int2)}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.error).toBeDefined();
            });

            expect(result.current.error).toContain('CAST_002');
        });

        it('should handle invalid UUID format', async () => {
            const shape = Shape.object({
                id: Shape.uuid4Value()
            });

            const {result} = renderHook(() =>
                useAdminOne(
                    `MAP {id: cast('invalid-uuid', uuid4)}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.error).toBeDefined();
            });
        });

        it('should handle invalid date format ', async () => {
            const shape = Shape.object({
                date: Shape.dateValue()
            });

            const {result} = renderHook(() =>
                useAdminOne(
                    `MAP {date: cast('not-a-date', date)}`,
                    undefined,
                    shape
                )
            );

            await waitFor(() => {
                expect(result.current.is_executing).toBe(false);
                expect(result.current.error).toBeDefined();
            });
        });
    });
});
