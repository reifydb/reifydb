// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useSubscriptionExecutor, getConnection, clearConnection, Schema } from '../../../src';
import { waitForDatabase } from '../setup';
import { createTestTableForHook } from './subscription-test-helpers';

describe('useSubscriptionExecutor - Value Object Schema Transformations', () => {
    beforeAll(async () => {
        await waitForDatabase();
        const conn = getConnection();
        await conn.connect();
    }, 30000);

    afterAll(async () => {
        await clearConnection();
    });

    describe('Integer Value Objects', () => {
        it('should transform to Int4 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_int4',
                ['id Int4', 'value Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), value: Schema.int4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, value: 2147483647 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.value).toBeDefined();
            expect(row.value.type).toBe('Int4');
            expect(typeof row.value.value).toBe('number');
        });

        it('should transform to Int8 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_int8',
                ['id Int4', 'bigValue Int8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), bigValue: Schema.int8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, bigValue: 9007199254740991 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.bigValue).toBeDefined();
            expect(row.bigValue.type).toBe('Int8');
            expect(typeof row.bigValue.value).toBe('bigint');
        });

        it('should transform to Uint4 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_uint4',
                ['id Int4', 'unsigned Uint4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), unsigned: Schema.uint4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, unsigned: 4294967295 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.unsigned).toBeDefined();
            expect(row.unsigned.type).toBe('Uint4');
            expect(typeof row.unsigned.value).toBe('number');
        });

        it('should transform to Uint8 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_uint8',
                ['id Int4', 'bigUnsigned Uint8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), bigUnsigned: Schema.uint8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, bigUnsigned: 18446744073709551615 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.bigUnsigned).toBeDefined();
            expect(row.bigUnsigned.type).toBe('Uint8');
            expect(typeof row.bigUnsigned.value).toBe('bigint');
        });
    });

    describe('Float Value Objects', () => {
        it('should transform to Float4 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_float4',
                ['id Int4', 'floatValue Float4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), floatValue: Schema.float4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, floatValue: 3.14 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.floatValue).toBeDefined();
            expect(row.floatValue.type).toBe('Float4');
            expect(typeof row.floatValue.value).toBe('number');
            expect(row.floatValue.value).toBeCloseTo(3.14, 2);
        });

        it('should transform to Float8 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_float8',
                ['id Int4', 'doubleValue Float8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), doubleValue: Schema.float8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, doubleValue: 2.718281828459045 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.doubleValue).toBeDefined();
            expect(row.doubleValue.type).toBe('Float8');
            expect(typeof row.doubleValue.value).toBe('number');
            expect(row.doubleValue.value).toBeCloseTo(2.718281828459045, 10);
        });
    });

    describe('String Value Objects', () => {
        it('should transform to Utf8 Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_utf8',
                ['id Int4', 'text Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), text: Schema.utf8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, text: 'Hello, World!' }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.text).toBeDefined();
            expect(row.text.type).toBe('Utf8');
            expect(typeof row.text.value).toBe('string');
            expect(row.text.value).toBe('Hello, World!');
        });
    });

    describe('Boolean Value Objects', () => {
        it('should transform to Boolean Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_bool',
                ['id Int4', 'flag Boolean']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), flag: Schema.booleanValue() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, flag: true }, { id: 2, flag: false }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const rows = result.current.state.changes[0].rows;
            expect(rows[0].flag).toBeDefined();
            expect(rows[0].flag.type).toBe('Boolean');
            expect(typeof rows[0].flag.value).toBe('boolean');
            expect(rows[0].flag.value).toBe(true);
            expect(rows[1].flag.value).toBe(false);
        });
    });

    describe('Mixed Value Objects', () => {
        it('should handle mixed Value object types', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_mixed',
                ['id Int4', 'count Int4', 'ratio Float8', 'name Utf8', 'active Boolean']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),
                        count: Schema.int4Value(),
                        ratio: Schema.float8Value(),
                        name: Schema.utf8Value(),
                        active: Schema.booleanValue()
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, count: 100, ratio: 0.95, name: 'Alice', active: true }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(row.id).toBe(1);
            expect(row.count.type).toBe('Int4');
            expect(row.count.value).toBe(100);
            expect(row.ratio.type).toBe('Float8');
            expect(row.ratio.value).toBeCloseTo(0.95, 2);
            expect(row.name.type).toBe('Utf8');
            expect(row.name.value).toBe('Alice');
            expect(row.active.type).toBe('Boolean');
            expect(row.active.value).toBe(true);
        });
    });

    describe('Operations with Value Objects', () => {
        it('should handle INSERT with Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_insert',
                ['id Int4', 'value Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), value: Schema.int4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, value: 42 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[0].rows[0].value.type).toBe('Int4');
            expect(result.current.state.changes[0].rows[0].value.value).toBe(42);
        });

        it('should handle UPDATE with Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_update',
                ['id Int4', 'score Float8']
            );

            // Subscribe FIRST (to empty table)
            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), score: Schema.float8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // INSERT after subscribing
            const client = getConnection().getClient();
            await act(async () => {
                await client!.command(`INSERT test::${tableName} [{ id: 1, score: 85.5 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // Now do UPDATE
            await act(async () => {
                await client!.command(`UPDATE test::${tableName} { score: 92.0 } FILTER id == 1`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('UPDATE');
            expect(result.current.state.changes[1].rows[0].score.type).toBe('Float8');
            expect(result.current.state.changes[1].rows[0].score.value).toBeCloseTo(92.0, 2);
        });

        it('should handle REMOVE with Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_remove',
                ['id Int4', 'name Utf8']
            );

            // Subscribe FIRST (to empty table)
            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), name: Schema.utf8Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // INSERT after subscribing
            const client = getConnection().getClient();
            await act(async () => {
                await client!.command(`INSERT test::${tableName} [{ id: 1, name: 'to_delete' }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // Now do REMOVE
            await act(async () => {
                await client!.command(`DELETE test::${tableName} FILTER id == 1`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            expect(result.current.state.changes[0].operation).toBe('INSERT');
            expect(result.current.state.changes[1].operation).toBe('REMOVE');
            expect(result.current.state.changes[1].rows[0].name.type).toBe('Utf8');
            expect(result.current.state.changes[1].rows[0].name.value).toBe('to_delete');
        });

        it('should maintain Value object types across multiple operations', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_multi_ops',
                ['id Int4', 'counter Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), counter: Schema.int4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // INSERT
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, counter: 0 }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            // UPDATE
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`UPDATE test::${tableName} { counter: 5 } FILTER id == 1`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(2);
            });

            // REMOVE
            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`DELETE test::${tableName} FILTER id == 1`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(3);
            });

            // All operations should preserve Value object type
            result.current.state.changes.forEach(change => {
                change.rows.forEach(row => {
                    expect(row.counter.type).toBe('Int4');
                    expect(typeof row.counter.value).toBe('number');
                });
            });
        });
    });

    describe('Value Property Access', () => {
        it('should verify .value property access on Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_access',
                ['id Int4', 'amount Int4', 'rate Float8', 'label Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),
                        amount: Schema.int4Value(),
                        rate: Schema.float8Value(),
                        label: Schema.utf8Value()
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, amount: 1000, rate: 0.05, label: 'Premium' }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];

            // Verify .value property access
            const amount = row.amount.value;
            const rate = row.rate.value;
            const label = row.label.value;

            expect(amount).toBe(1000);
            expect(rate).toBeCloseTo(0.05, 2);
            expect(label).toBe('Premium');

            // Can perform operations on .value
            const total = amount * (1 + rate);
            expect(total).toBeCloseTo(1050, 2);
        });

        it('should handle batch operations with Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_batch',
                ['id Int4', 'value Int4']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({ id: Schema.number(), value: Schema.int4Value() })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            // Insert multiple rows
            const rows = Array.from({ length: 10 }, (_, i) => ({
                id: i,
                value: i * 10
            }));

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} FROM ${JSON.stringify(rows)}`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const insertedRows = result.current.state.changes[0].rows;
            expect(insertedRows).toHaveLength(10);

            insertedRows.forEach((row, idx) => {
                expect(row.value.type).toBe('Int4');
                expect(row.value.value).toBe(idx * 10);
            });
        });
    });

    describe('Edge Cases', () => {
        it('should handle mixed primitives and Value objects', async () => {
            const { result } = renderHook(() => useSubscriptionExecutor());
            const tableName = await createTestTableForHook(
                'val_mixed_schema',
                ['id Int4', 'count Int4', 'name Utf8']
            );

            await act(async () => {
                await result.current.subscribe(
                    `from test::${tableName}`,
                    null,
                    Schema.object({
                        id: Schema.number(),           // primitive
                        count: Schema.int4Value(),     // Value object
                        name: Schema.string()          // primitive
                    })
                );
            });

            await waitFor(() => {
                expect(result.current.state.isSubscribed).toBe(true);
            });

            await act(async () => {
                const client = getConnection().getClient();
                await client!.command(`INSERT test::${tableName} [{ id: 1, count: 100, name: 'Test' }]`, null, []);
            });

            await waitFor(() => {
                expect(result.current.state.changes.length).toBe(1);
            });

            const row = result.current.state.changes[0].rows[0];
            expect(typeof row.id).toBe('number');           // primitive
            expect(row.count.type).toBe('Int4');            // Value object
            expect(typeof row.name).toBe('string');         // primitive
        });
    });
});
