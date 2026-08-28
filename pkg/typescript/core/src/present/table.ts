// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {formatValue, valueRole, valueTypeName, type ValueRole} from './value';
import {displayWidth, truncateToWidth} from './width';

export type ColumnAlign = 'left' | 'right';

export interface ResultColumn {
    name: string;
    type?: string;
    role: ValueRole;
    align: ColumnAlign;
}

export interface PlannedColumn {
    column: ResultColumn;
    width: number;
}

export interface TablePlan {
    columns: PlannedColumn[];
    dropped: ResultColumn[];
    truncated: boolean;
}

export interface TableLayoutOptions {
    width?: number;
    maxColumnWidth?: number;
    sample?: number;
    columnOverhead?: number;
    tableOverhead?: number;
}

const DEFAULT_MAX_COLUMN_WIDTH = 40;
const DEFAULT_COLUMN_OVERHEAD = 3;
const DEFAULT_TABLE_OVERHEAD = 1;

export function inferColumns(rows: ReadonlyArray<Record<string, unknown>>): ResultColumn[] {
    const names: string[] = [];
    const seen = new Set<string>();

    for (const row of rows) {
        for (const name of Object.keys(row)) {
            if (seen.has(name)) continue;
            seen.add(name);
            names.push(name);
        }
    }

    return names.map((name) => {
        let role: ValueRole = 'unknown';
        let type: string | undefined;

        for (const row of rows) {
            const value = row[name];
            const candidate = valueRole(value);
            if (candidate === 'none' || candidate === 'unknown') continue;
            role = candidate;
            type = valueTypeName(value);
            break;
        }

        return {name, type, role, align: role === 'number' ? 'right' : 'left'};
    });
}

export function planTable(
    rows: ReadonlyArray<Record<string, unknown>>,
    columns: ReadonlyArray<ResultColumn>,
    options: TableLayoutOptions = {},
): TablePlan {
    const maxColumnWidth = options.maxColumnWidth ?? DEFAULT_MAX_COLUMN_WIDTH;
    const columnOverhead = options.columnOverhead ?? DEFAULT_COLUMN_OVERHEAD;
    const tableOverhead = options.tableOverhead ?? DEFAULT_TABLE_OVERHEAD;
    const sample = options.sample ?? rows.length;
    const measured = rows.slice(0, Math.max(sample, 0));

    let truncated = false;

    const natural = columns.map((column) => {
        let width = displayWidth(column.name);
        for (const row of measured) {
            width = Math.max(width, displayWidth(formatValue(row[column.name])));
        }
        if (width > maxColumnWidth) truncated = true;
        return {column, width: Math.min(width, maxColumnWidth)};
    });

    if (options.width === undefined) {
        return {columns: natural, dropped: [], truncated};
    }

    const planned: PlannedColumn[] = [];
    const dropped: ResultColumn[] = [];
    let used = tableOverhead;

    for (const entry of natural) {
        const required = entry.width + columnOverhead;
        if (planned.length > 0 && used + required > options.width) {
            dropped.push(entry.column);
            continue;
        }
        if (planned.length === 0 && used + required > options.width) {
            const available = options.width - used - columnOverhead;
            const width = Math.max(available, 1);
            if (width < entry.width) truncated = true;
            planned.push({column: entry.column, width});
            used += width + columnOverhead;
            continue;
        }
        planned.push(entry);
        used += required;
    }

    return {columns: planned, dropped, truncated};
}

export function cellText(value: unknown, width: number): string {
    return truncateToWidth(formatValue(value), width);
}
