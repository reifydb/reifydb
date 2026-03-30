// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import {useEffect, useState} from 'react';
import {Shape, InferShape} from '@reifydb/core';
import {useQueryExecutor} from './use-query-executor';

export interface ColumnInfo {
    name: string;
    dataType: string;
}

export interface TableInfo {
    name: string;
    columns: ColumnInfo[];
}

const namespaceShape = Shape.object({
    id: Shape.number(),
    name: Shape.string(),
});

const tableShape = Shape.object({
    id: Shape.number(),
    namespace_id: Shape.number(),
    name: Shape.string(),
    primary_key_id: Shape.number(),
});

const viewShape = Shape.object({
    id: Shape.number(),
    namespace_id: Shape.number(),
    name: Shape.string(),
});

const columnShape = Shape.object({
    id: Shape.number(),
    shape_id: Shape.number(),
    shape_type: Shape.number(),
    name: Shape.string(),
    type: Shape.number(),
    position: Shape.number(),
    auto_increment: Shape.boolean(),
});

type NamespaceRow = InferShape<typeof namespaceShape>;
type TableRow = InferShape<typeof tableShape>;
type ViewRow = InferShape<typeof viewShape>;
type ColumnRow = InferShape<typeof columnShape>;

export function useShape(): [boolean, TableInfo[], string | undefined] {
    const {isExecuting, results, error, query} = useQueryExecutor();
    const [shape, setShape] = useState<TableInfo[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        if (!query) return;

        const fetchShape = async () => {
            setIsLoading(true);

            try {
                await query(
                    `OUTPUT FROM system::namespaces; OUTPUT FROM system::tables; OUTPUT FROM system::views; OUTPUT FROM system::columns;`,
                    undefined,
                    [namespaceShape, tableShape, viewShape, columnShape]
                );
            } catch (err) {
                console.error('Failed to fetch shape:', err);
            }
        };

        fetchShape();
    }, [query]);

    useEffect(() => {
        if (!results || results.length < 4) {
            setIsLoading(isExecuting);
            return;
        }

        const tablesResult = results[1];
        const viewsResult = results[2];
        const columnsResult = results[3];

        if (!tablesResult?.rows || !viewsResult?.rows || !columnsResult?.rows) {
            setIsLoading(false);
            return;
        }

        const namespacesResult = results[0];
        const namespaces = namespacesResult.rows as unknown as NamespaceRow[];
        const tables = tablesResult.rows as unknown as TableRow[];
        const views = viewsResult.rows as unknown as ViewRow[];
        const columns = columnsResult.rows as unknown as ColumnRow[];

        const namespaceMap = new Map<number, string>();
        namespaces.forEach((ns) => {
            const id = ns.id?.valueOf() as number;
            const name = ns.name?.valueOf() as string;
            if (id !== undefined && name) {
                namespaceMap.set(id, name);
            }
        });

        const tableInfoMap = new Map<number, TableInfo>();

        tables.forEach((table) => {
            const tableId = table.id?.valueOf() as number;
            const namespaceId = table.namespace_id?.valueOf() as number;
            const tableName = table.name?.valueOf() as string;

            if (tableId === undefined || !tableName || namespaceId === undefined) return;

            const namespace = namespaceMap.get(namespaceId);
            if (!namespace) return;

            const fullTableName = `${namespace}::${tableName}`;

            tableInfoMap.set(tableId, {
                name: fullTableName,
                columns: [],
            });
        });

        views.forEach((view) => {
            const viewId = view.id?.valueOf() as number;
            const namespaceId = view.namespace_id?.valueOf() as number;
            const viewName = view.name?.valueOf() as string;

            if (viewId === undefined || !viewName || namespaceId === undefined) return;

            const namespace = namespaceMap.get(namespaceId);
            if (!namespace) return;

            const fullViewName = `${namespace}::${viewName}`;

            tableInfoMap.set(viewId, {
                name: fullViewName,
                columns: [],
            });
        });

        const typeMap: Record<number, string> = {
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
        const tableColumnsMap = new Map<number, Array<{name: string; dataType: string; position: number}>>();

        columns.forEach((column) => {
            const shapeId = column.shape_id?.valueOf() as number;
            const shapeType = column.shape_type?.valueOf() as number;
            const columnName = column.name?.valueOf() as string;
            const typeId = column.type?.valueOf() as number;
            const position = column.position?.valueOf() as number;

            if (shapeId === undefined || !columnName || typeId === undefined) return;
            if (shapeType !== 0 && shapeType !== 1) return;

            if (!tableColumnsMap.has(shapeId)) {
                tableColumnsMap.set(shapeId, []);
            }

            tableColumnsMap.get(shapeId)!.push({
                name: columnName,
                dataType: typeMap[typeId] || `Unknown(${typeId})`,
                position: position ?? 0,
            });
        });

        // Sort columns by position and add to table info
        tableColumnsMap.forEach((cols, shapeId) => {
            const tableInfo = tableInfoMap.get(shapeId);
            if (tableInfo) {
                cols.sort((a, b) => a.position - b.position);
                tableInfo.columns = cols.map((c) => ({name: c.name, dataType: c.dataType}));
            }
        });

        const shapeArray = Array.from(tableInfoMap.values())
            .filter((table) => table.name !== 'reifydb::flows')
            .sort((a, b) => a.name.localeCompare(b.name));

        setShape(shapeArray);
        setIsLoading(false);
    }, [results, isExecuting]);

    return [isLoading, shape, error];
}
