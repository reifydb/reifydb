// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useEffect, useState} from 'react';
import {Shape, InferShape} from '@reifydb/core';
import {useQueryExecutor} from './use-query-executor';

export interface ColumnInfo {
    name: string;
    data_type: string;
}

export interface TableInfo {
    name: string;
    columns: ColumnInfo[];
}

const namespace_shape = Shape.object({
    id: Shape.number(),
    name: Shape.string(),
});

const table_shape = Shape.object({
    id: Shape.number(),
    namespace_id: Shape.number(),
    name: Shape.string(),
    primary_key_id: Shape.number(),
});

const view_shape = Shape.object({
    id: Shape.number(),
    namespace_id: Shape.number(),
    name: Shape.string(),
});

const column_shape = Shape.object({
    id: Shape.number(),
    shape_id: Shape.number(),
    shape_type: Shape.number(),
    name: Shape.string(),
    type: Shape.number(),
    position: Shape.number(),
    auto_increment: Shape.boolean(),
});

type NamespaceRow = InferShape<typeof namespace_shape>;
type TableRow = InferShape<typeof table_shape>;
type ViewRow = InferShape<typeof view_shape>;
type ColumnRow = InferShape<typeof column_shape>;

export function useShape(): [boolean, TableInfo[], string | undefined] {
    const {is_executing, results, error, query} = useQueryExecutor();
    const [shape, set_shape] = useState<TableInfo[]>([]);
    const [is_loading, set_is_loading] = useState(true);

    useEffect(() => {
        if (!query) return;

        const fetch_shape = async () => {
            set_is_loading(true);

            try {
                await query(
                    `OUTPUT FROM system::namespaces; OUTPUT FROM system::tables; OUTPUT FROM system::views; OUTPUT FROM system::columns;`,
                    undefined,
                    [namespace_shape, table_shape, view_shape, column_shape]
                );
            } catch (err) {
                console.error('Failed to fetch shape:', err);
            }
        };

        fetch_shape();
    }, [query]);

    useEffect(() => {
        if (!results || results.length < 4) {
            set_is_loading(is_executing);
            return;
        }

        const tables_result = results[1];
        const views_result = results[2];
        const columns_result = results[3];

        if (!tables_result?.rows || !views_result?.rows || !columns_result?.rows) {
            set_is_loading(false);
            return;
        }

        const namespaces_result = results[0];
        const namespaces = namespaces_result.rows as unknown as NamespaceRow[];
        const tables = tables_result.rows as unknown as TableRow[];
        const views = views_result.rows as unknown as ViewRow[];
        const columns = columns_result.rows as unknown as ColumnRow[];

        const namespace_map = new Map<number, string>();
        namespaces.forEach((ns) => {
            const id = ns.id?.valueOf() as number;
            const name = ns.name?.valueOf() as string;
            if (id !== undefined && name) {
                namespace_map.set(id, name);
            }
        });

        const table_info_map = new Map<number, TableInfo>();

        tables.forEach((table) => {
            const table_id = table.id?.valueOf() as number;
            const namespace_id = table.namespace_id?.valueOf() as number;
            const table_name = table.name?.valueOf() as string;

            if (table_id === undefined || !table_name || namespace_id === undefined) return;

            const namespace = namespace_map.get(namespace_id);
            if (!namespace) return;

            const full_table_name = `${namespace}::${table_name}`;

            table_info_map.set(table_id, {
                name: full_table_name,
                columns: [],
            });
        });

        views.forEach((view) => {
            const view_id = view.id?.valueOf() as number;
            const namespace_id = view.namespace_id?.valueOf() as number;
            const view_name = view.name?.valueOf() as string;

            if (view_id === undefined || !view_name || namespace_id === undefined) return;

            const namespace = namespace_map.get(namespace_id);
            if (!namespace) return;

            const full_view_name = `${namespace}::${view_name}`;

            table_info_map.set(view_id, {
                name: full_view_name,
                columns: [],
            });
        });

        const type_map: Record<number, string> = {
            0: 'None',
            1: 'Float4',
            2: 'Float8',
            3: 'Int1',
            4: 'Int2',
            5: 'Int4',
            6: 'Int8',
            7: 'Int16',
            8: 'Utf8',
            9: 'Uint1',
            10: 'Uint2',
            11: 'Uint4',
            12: 'Uint8',
            13: 'Uint16',
            14: 'Boolean',
            15: 'Date',
            16: 'DateTime',
            17: 'Time',
            18: 'Duration',
            19: 'IdentityId',
            20: 'Uuid4',
            21: 'Uuid7',
            22: 'Blob',
            23: 'Int',
            24: 'Decimal',
            25: 'Uint',
            26: 'Any',
        };

        // Create a map to collect columns with their positions
        const table_columns_map = new Map<number, Array<{name: string; data_type: string; position: number}>>();

        columns.forEach((column) => {
            const shape_id = column.shape_id?.valueOf() as number;
            const shape_type = column.shape_type?.valueOf() as number;
            const column_name = column.name?.valueOf() as string;
            const type_id = column.type?.valueOf() as number;
            const position = column.position?.valueOf() as number;

            if (shape_id === undefined || !column_name || type_id === undefined) return;
            if (shape_type !== 0 && shape_type !== 1) return;

            if (!table_columns_map.has(shape_id)) {
                table_columns_map.set(shape_id, []);
            }

            table_columns_map.get(shape_id)!.push({
                name: column_name,
                data_type: type_map[type_id] || `Unknown(${type_id})`,
                position: position ?? 0,
            });
        });

        // Sort columns by position and add to table info
        table_columns_map.forEach((cols, shape_id) => {
            const table_info = table_info_map.get(shape_id);
            if (table_info) {
                cols.sort((a, b) => a.position - b.position);
                table_info.columns = cols.map((c) => ({name: c.name, data_type: c.data_type}));
            }
        });

        const shape_array = Array.from(table_info_map.values())
            .filter((table) => table.name !== 'reifydb::flows')
            .sort((a, b) => a.name.localeCompare(b.name));

        set_shape(shape_array);
        set_is_loading(false);
    }, [results, is_executing]);

    return [is_loading, shape, error];
}
