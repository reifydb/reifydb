// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '../../src';
import {connect, namespace, waitFor} from './setup';
import {createTable, rowsOf, tableName, waitForReady, waitForRows} from './subscription-helpers';

const ns = namespace('sub_vo');

describe.each(['frames', 'rbcf'] as const)('subscription value object shapes (%s)', format => {
    let client: WsClient;
    let store: Store;

    beforeAll(async () => {
        client = await connect(format);
        await client.admin(`create namespace ${ns}`, null, []).catch(() => undefined);
        store = new Store(client);
    });

    afterAll(async () => {
        store.reset();
        await client.disconnect();
    });

    async function roundTrip(prefix: string, columns: string, shape: any, values: string): Promise<any> {
        const name = `${ns}::${tableName(`${prefix}_${format}`)}`;
        await createTable(client, name, columns);
        const rql = `from ${name}`;
        const release = store.subscribe(rql, null, shape);
        await waitForReady(store, rql, shape);
        await client.command(`insert ${name} [${values}]`, null, []);
        await waitForRows(store, rql, shape, 1);
        const row = rowsOf(store, rql, shape)[0];
        release();
        return row;
    }

    describe('integer value objects', () => {
        it('Int4 arrives as a value object carrying its own type tag', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.int4Value()});
            const row = await roundTrip('vo_int4', 'id: int4, value: int4', shape, '{ id: 1, value: 42 }');
            expect(row.value.type).toBe('Int4');
            expect(row.value.value).toBe(42);
        });

        it('Int8 keeps full precision inside the value object', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.int8Value()});
            const row = await roundTrip('vo_int8', 'id: int4, value: int8', shape, '{ id: 1, value: 9007199254740993 }');
            expect(row.value.type).toBe('Int8');
            expect(row.value.value).toBe(9007199254740993n);
        });

        it('Uint4 and Uint8 arrive as their own value object types', async () => {
            const shape = Shape.object({id: Shape.int4(), u4: Shape.uint4Value(), u8: Shape.uint8Value()});
            const row = await roundTrip(
                'vo_uints',
                'id: int4, u4: uint4, u8: uint8',
                shape,
                '{ id: 1, u4: 4294967295, u8: 18446744073709551615 }'
            );
            expect(row.u4.type).toBe('Uint4');
            expect(row.u4.value).toBe(4294967295);
            expect(row.u8.type).toBe('Uint8');
            expect(row.u8.value).toBe(18446744073709551615n);
        });

        it('Int1 and Int2 arrive as their own value object types', async () => {
            const shape = Shape.object({id: Shape.int4(), i1: Shape.int1Value(), i2: Shape.int2Value()});
            const row = await roundTrip(
                'vo_small',
                'id: int4, i1: int1, i2: int2',
                shape,
                '{ id: 1, i1: -128, i2: 32767 }'
            );
            expect(row.i1.type).toBe('Int1');
            expect(row.i1.value).toBe(-128);
            expect(row.i2.type).toBe('Int2');
            expect(row.i2.value).toBe(32767);
        });
    });

    describe('float value objects', () => {
        it('Float4 and Float8 arrive as their own value object types', async () => {
            const shape = Shape.object({id: Shape.int4(), f4: Shape.float4Value(), f8: Shape.float8Value()});
            const row = await roundTrip(
                'vo_floats',
                'id: int4, f4: float4, f8: float8',
                shape,
                '{ id: 1, f4: 1.5, f8: 3.25 }'
            );
            expect(row.f4.type).toBe('Float4');
            expect(row.f4.value).toBeCloseTo(1.5, 5);
            expect(row.f8.type).toBe('Float8');
            expect(row.f8.value).toBeCloseTo(3.25, 10);
        });
    });

    describe('string and boolean value objects', () => {
        it('Utf8 arrives as a value object wrapping the string', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.utf8Value()});
            const row = await roundTrip('vo_utf8', 'id: int4, value: utf8', shape, "{ id: 1, value: 'hello' }");
            expect(row.value.type).toBe('Utf8');
            expect(row.value.value).toBe('hello');
        });

        it('Boolean arrives as a value object wrapping the boolean', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.booleanValue()});
            const row = await roundTrip('vo_bool', 'id: int4, value: bool', shape, '{ id: 1, value: true }');
            expect(row.value.type).toBe('Boolean');
            expect(row.value.value).toBe(true);
        });
    });

    describe('temporal and identifier value objects', () => {
        it('Date, Time, DateTime and Duration arrive as their own value object types', async () => {
            const shape = Shape.object({
                id: Shape.int4(),
                d: Shape.dateValue(),
                t: Shape.timeValue(),
                dt: Shape.dateTimeValue(),
                dur: Shape.durationValue(),
            });
            const row = await roundTrip(
                'vo_temporal',
                'id: int4, d: date, t: time, dt: datetime, dur: duration',
                shape,
                "{ id: 1, d: '2024-03-15', t: '12:30:45', dt: '2024-03-15T12:30:45Z', dur: 'PT1H' }"
            );
            expect(row.d.type).toBe('Date');
            expect(row.t.type).toBe('Time');
            expect(row.dt.type).toBe('DateTime');
            expect(row.dur.type).toBe('Duration');
        });

        it('Uuid4 and Uuid7 arrive as their own value object types', async () => {
            const shape = Shape.object({id: Shape.int4(), u4: Shape.uuid4Value(), u7: Shape.uuid7Value()});
            const row = await roundTrip(
                'vo_uuid',
                'id: int4, u4: uuid4, u7: uuid7',
                shape,
                "{ id: 1, u4: '550e8400-e29b-41d4-a716-446655440000', u7: '018f4e5a-0000-7000-8000-000000000000' }"
            );
            expect(row.u4.type).toBe('Uuid4');
            expect(row.u7.type).toBe('Uuid7');
        });

        it('Blob arrives as a value object', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.blobValue()});
            const row = await roundTrip(
                'vo_blob',
                'id: int4, value: blob',
                shape,
                "{ id: 1, value: cast('deadbeef', blob) }"
            );
            expect(row.value.type).toBe('Blob');
        });
    });

    describe('mixed and operations', () => {
        it('primitives and value objects can be mixed in one shape', async () => {
            const shape = Shape.object({id: Shape.int4(), plain: Shape.utf8(), wrapped: Shape.int4Value()});
            const row = await roundTrip(
                'vo_mixed',
                'id: int4, plain: utf8, wrapped: int4',
                shape,
                "{ id: 1, plain: 'text', wrapped: 5 }"
            );
            expect(typeof row.plain).toBe('string');
            expect(row.wrapped.type).toBe('Int4');
            expect(row.wrapped.value).toBe(5);
        });

        it('an UPDATE delivers a value object just as the INSERT did', async () => {
            const name = `${ns}::${tableName(`vo_upd_${format}`)}`;
            await createTable(client, name, 'id: int4, value: int4');
            const shape = Shape.object({id: Shape.int4(), value: Shape.int4Value()});
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, shape);
            await waitForReady(store, rql, shape);

            await client.command(`insert ${name} [{ id: 1, value: 1 }]`, null, []);
            await waitForRows(store, rql, shape, 1);
            expect(rowsOf(store, rql, shape)[0].value.type).toBe('Int4');

            await client.command(`update ${name} { value: 99 } filter id == 1`, null, []);
            await waitFor(store, () => rowsOf(store, rql, shape)[0]?.value?.value === 99);
            expect(rowsOf(store, rql, shape)[0].value.type).toBe('Int4');
            release();
        });

        it('a batch of rows all arrive as value objects', async () => {
            const name = `${ns}::${tableName(`vo_batch_${format}`)}`;
            await createTable(client, name, 'id: int4, value: int4');
            const shape = Shape.object({id: Shape.int4(), value: Shape.int4Value()});
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, shape);
            await waitForReady(store, rql, shape);

            const rows = Array.from({length: 20}, (_, i) => `{ id: ${i}, value: ${i * 2} }`).join(', ');
            await client.command(`insert ${name} [${rows}]`, null, []);
            await waitForRows(store, rql, shape, 20);
            const decoded = rowsOf(store, rql, shape);
            expect(decoded.every(row => row.value.type === 'Int4')).toBe(true);
            release();
        });
    });
});
