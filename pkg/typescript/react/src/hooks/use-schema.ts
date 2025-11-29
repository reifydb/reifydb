import {useEffect, useState} from 'react';
import {Schema, InferSchema} from '@reifydb/core';
import {useQueryExecutor} from './use-query-executor';

export interface ColumnInfo {
    name: string;
    dataType: string;
}

export interface TableInfo {
    name: string;
    columns: ColumnInfo[];
}

const namespaceSchema = Schema.object({
    id: Schema.number(),
    name: Schema.string(),
});

const tableSchema = Schema.object({
    id: Schema.number(),
    namespace_id: Schema.number(),
    name: Schema.string(),
    primary_key_id: Schema.number(),
});

const viewSchema = Schema.object({
    id: Schema.number(),
    namespace_id: Schema.number(),
    name: Schema.string(),
});

const columnSchema = Schema.object({
    id: Schema.number(),
    source_id: Schema.number(),
    source_type: Schema.number(),
    name: Schema.string(),
    type: Schema.number(),
    position: Schema.number(),
    auto_increment: Schema.boolean(),
});

type NamespaceRow = InferSchema<typeof namespaceSchema>;
type TableRow = InferSchema<typeof tableSchema>;
type ViewRow = InferSchema<typeof viewSchema>;
type ColumnRow = InferSchema<typeof columnSchema>;

export function useSchema(): [boolean, TableInfo[], string | undefined] {
    const {isExecuting, results, error, query} = useQueryExecutor();
    const [schema, setSchema] = useState<TableInfo[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        if (!query) return;

        const fetchSchema = async () => {
            setIsLoading(true);

            try {
                await query(
                    `FROM system.namespaces; FROM system.tables; FROM system.views; FROM system.columns;`,
                    undefined,
                    [namespaceSchema, tableSchema, viewSchema, columnSchema]
                );
            } catch (err) {
                console.error('Failed to fetch schema:', err);
            }
        };

        fetchSchema();
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

            const fullTableName = `${namespace}.${tableName}`;

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

            const fullViewName = `${namespace}.${viewName}`;

            tableInfoMap.set(viewId, {
                name: fullViewName,
                columns: [],
            });
        });

        const typeMap: Record<number, string> = {
            0: 'Undefined',
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
            19: 'RowNumber',
            20: 'Uuid4',
            21: 'Uuid7',
            22: 'Blob',
            23: 'IdentityId',
            24: 'Int',
            25: 'Decimal',
            26: 'Uint',
            27: 'Any',
        };

        // Create a map to collect columns with their positions
        const tableColumnsMap = new Map<number, Array<{name: string; dataType: string; position: number}>>();

        columns.forEach((column) => {
            const sourceId = column.source_id?.valueOf() as number;
            const sourceType = column.source_type?.valueOf() as number;
            const columnName = column.name?.valueOf() as string;
            const typeId = column.type?.valueOf() as number;
            const position = column.position?.valueOf() as number;

            if (sourceId === undefined || !columnName || typeId === undefined) return;
            if (sourceType !== 0 && sourceType !== 1) return;

            if (!tableColumnsMap.has(sourceId)) {
                tableColumnsMap.set(sourceId, []);
            }

            tableColumnsMap.get(sourceId)!.push({
                name: columnName,
                dataType: typeMap[typeId] || `Unknown(${typeId})`,
                position: position ?? 0,
            });
        });

        // Sort columns by position and add to table info
        tableColumnsMap.forEach((cols, sourceId) => {
            const tableInfo = tableInfoMap.get(sourceId);
            if (tableInfo) {
                cols.sort((a, b) => a.position - b.position);
                tableInfo.columns = cols.map((c) => ({name: c.name, dataType: c.dataType}));
            }
        });

        const schemaArray = Array.from(tableInfoMap.values())
            .filter((table) => table.name !== 'reifydb.flows')
            .sort((a, b) => a.name.localeCompare(b.name));

        setSchema(schemaArray);
        setIsLoading(false);
    }, [results, isExecuting]);

    return [isLoading, schema, error];
}
