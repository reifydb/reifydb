// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {Shape} from '@reifydb/core';
import type {WsClient} from '@reifydb/client';
import {Store} from '../../src';
import {connect, namespace, waitFor} from './setup';
import {createTable, rowsOf, tableName, waitForReady, waitForRows} from './subscription-helpers';

const ns = namespace('sub_prim');

describe.each(['frames', 'rbcf'] as const)('subscription primitive shapes (%s)', format => {
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

    // Subscribes, writes one row, and hands back the row as the shape decoded it.
    async function roundTrip<S extends Parameters<typeof Shape.object>[0]>(
        prefix: string,
        columns: string,
        shape: ReturnType<typeof Shape.object>,
        values: string
    ): Promise<any> {
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

    describe('number types', () => {
        it('Int4 arrives as a number', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.int4()});
            const row = await roundTrip('int4', 'id: int4, value: int4', shape, '{ id: 1, value: 42 }');
            expect(typeof row.value).toBe('number');
            expect(row.value).toBe(42);
        });

        it('Int8 arrives as a bigint, so values past 2^53 are not silently rounded', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.int8()});
            const row = await roundTrip('int8', 'id: int4, value: int8', shape, '{ id: 1, value: 9007199254740993 }');
            expect(typeof row.value).toBe('bigint');
            expect(row.value).toBe(9007199254740993n);
        });

        it('Int1 and Int2 arrive as numbers at their bounds', async () => {
            const shape = Shape.object({id: Shape.int4(), small: Shape.int1(), medium: Shape.int2()});
            const row = await roundTrip(
                'int12',
                'id: int4, small: int1, medium: int2',
                shape,
                '{ id: 1, small: -128, medium: 32767 }'
            );
            expect(row.small).toBe(-128);
            expect(row.medium).toBe(32767);
        });

        it('Float4 and Float8 arrive as numbers', async () => {
            const shape = Shape.object({id: Shape.int4(), f4: Shape.float4(), f8: Shape.float8()});
            const row = await roundTrip(
                'floats',
                'id: int4, f4: float4, f8: float8',
                shape,
                '{ id: 1, f4: 1.5, f8: 3.25 }'
            );
            expect(typeof row.f4).toBe('number');
            expect(row.f4).toBeCloseTo(1.5, 5);
            expect(row.f8).toBeCloseTo(3.25, 10);
        });

        it('Uint4 and Uint8 arrive with their unsigned range intact', async () => {
            const shape = Shape.object({id: Shape.int4(), u4: Shape.uint4(), u8: Shape.uint8()});
            const row = await roundTrip(
                'uints',
                'id: int4, u4: uint4, u8: uint8',
                shape,
                '{ id: 1, u4: 4294967295, u8: 18446744073709551615 }'
            );
            expect(row.u4).toBe(4294967295);
            expect(row.u8).toBe(18446744073709551615n);
        });
    });

    describe('string and boolean types', () => {
        it('Utf8 arrives as a string', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.utf8()});
            const row = await roundTrip('utf8', 'id: int4, value: utf8', shape, "{ id: 1, value: 'hello' }");
            expect(typeof row.value).toBe('string');
            expect(row.value).toBe('hello');
        });

        it('a unicode string survives the diff path byte for byte', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.utf8()});
            const row = await roundTrip('unicode', 'id: int4, value: utf8', shape, "{ id: 1, value: 'héllo 世界 🚀' }");
            expect(row.value).toBe('héllo 世界 🚀');
        });

        it('an empty string stays an empty string and does not become a none', async () => {
            const shape = Shape.object({id: Shape.int4(), value: Shape.utf8()});
            const row = await roundTrip('emptystr', 'id: int4, value: utf8', shape, "{ id: 1, value: '' }");
            expect(row.value).toBe('');
        });

        it('Boolean arrives as a boolean', async () => {
            const shape = Shape.object({id: Shape.int4(), yes: Shape.bool(), no: Shape.bool()});
            const row = await roundTrip(
                'bool',
                'id: int4, yes: bool, no: bool',
                shape,
                '{ id: 1, yes: true, no: false }'
            );
            expect(row.yes).toBe(true);
            expect(row.no).toBe(false);
        });
    });

    describe('mixed objects', () => {
        it('a row of mixed primitive types decodes every column to its own type', async () => {
            const shape = Shape.object({
                id: Shape.int4(),
                name: Shape.utf8(),
                score: Shape.float8(),
                active: Shape.bool(),
                big: Shape.int8(),
            });
            const row = await roundTrip(
                'mixed',
                'id: int4, name: utf8, score: float8, active: bool, big: int8',
                shape,
                "{ id: 7, name: 'seven', score: 1.5, active: true, big: 900719925474099 }"
            );
            expect(row).toEqual({id: 7, name: 'seven', score: 1.5, active: true, big: 900719925474099n});
        });
    });

    describe('transformations across operations', () => {
        it('an UPDATE decodes through the shape the same way an INSERT does', async () => {
            const name = `${ns}::${tableName(`upd_${format}`)}`;
            await createTable(client, name, 'id: int4, value: int8');
            const shape = Shape.object({id: Shape.int4(), value: Shape.int8()});
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, shape);
            await waitForReady(store, rql, shape);

            await client.command(`insert ${name} [{ id: 1, value: 1 }]`, null, []);
            await waitForRows(store, rql, shape, 1);
            expect(rowsOf(store, rql, shape)[0].value).toBe(1n);

            await client.command(`update ${name} { value: 9007199254740993 } filter id == 1`, null, []);
            await waitFor(store, () => rowsOf(store, rql, shape)[0]?.value === 9007199254740993n);
            const updated = rowsOf(store, rql, shape)[0];
            expect(typeof updated.value).toBe('bigint');
            release();
        });

        it('types stay consistent across a long run of operations', async () => {
            const name = `${ns}::${tableName(`consistent_${format}`)}`;
            await createTable(client, name, 'id: int4, value: float8');
            const shape = Shape.object({id: Shape.int4(), value: Shape.float8()});
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, shape);
            await waitForReady(store, rql, shape);

            for (let i = 1; i <= 5; i++) {
                await client.command(`insert ${name} [{ id: ${i}, value: ${i}.5 }]`, null, []);
            }
            await waitForRows(store, rql, shape, 5);
            for (const row of rowsOf(store, rql, shape)) {
                expect(typeof row.id).toBe('number');
                expect(typeof row.value).toBe('number');
            }
            release();
        });

        it('a large batch keeps every row correctly typed', async () => {
            const name = `${ns}::${tableName(`large_${format}`)}`;
            await createTable(client, name, 'id: int4, value: utf8');
            const shape = Shape.object({id: Shape.int4(), value: Shape.utf8()});
            const rql = `from ${name}`;
            const release = store.subscribe(rql, null, shape);
            await waitForReady(store, rql, shape);

            const rows = Array.from({length: 100}, (_, i) => `{ id: ${i}, value: 'v${i}' }`).join(', ');
            await client.command(`insert ${name} [${rows}]`, null, []);
            await waitForRows(store, rql, shape, 100);
            const decoded = rowsOf(store, rql, shape);
            expect(decoded).toHaveLength(100);
            expect(decoded.every(row => typeof row.id === 'number' && typeof row.value === 'string')).toBe(true);
            release();
        });
    });
});
