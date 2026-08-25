// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {format_value, value_role, value_type_name, type ValueRole} from './value';
import {display_width, truncate_to_width} from './width';

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
    max_column_width?: number;
    sample?: number;
    column_overhead?: number;
    table_overhead?: number;
}

const DEFAULT_MAX_COLUMN_WIDTH = 40;
const DEFAULT_COLUMN_OVERHEAD = 3;
const DEFAULT_TABLE_OVERHEAD = 1;

export function infer_columns(rows: ReadonlyArray<Record<string, unknown>>): ResultColumn[] {
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
            const candidate = value_role(value);
            if (candidate === 'none' || candidate === 'unknown') continue;
            role = candidate;
            type = value_type_name(value);
            break;
        }

        return {name, type, role, align: role === 'number' ? 'right' : 'left'};
    });
}

export function plan_table(
    rows: ReadonlyArray<Record<string, unknown>>,
    columns: ReadonlyArray<ResultColumn>,
    options: TableLayoutOptions = {},
): TablePlan {
    const max_column_width = options.max_column_width ?? DEFAULT_MAX_COLUMN_WIDTH;
    const column_overhead = options.column_overhead ?? DEFAULT_COLUMN_OVERHEAD;
    const table_overhead = options.table_overhead ?? DEFAULT_TABLE_OVERHEAD;
    const sample = options.sample ?? rows.length;
    const measured = rows.slice(0, Math.max(sample, 0));

    let truncated = false;

    const natural = columns.map((column) => {
        let width = display_width(column.name);
        for (const row of measured) {
            width = Math.max(width, display_width(format_value(row[column.name])));
        }
        if (width > max_column_width) truncated = true;
        return {column, width: Math.min(width, max_column_width)};
    });

    if (options.width === undefined) {
        return {columns: natural, dropped: [], truncated};
    }

    const planned: PlannedColumn[] = [];
    const dropped: ResultColumn[] = [];
    let used = table_overhead;

    for (const entry of natural) {
        const required = entry.width + column_overhead;
        if (planned.length > 0 && used + required > options.width) {
            dropped.push(entry.column);
            continue;
        }
        if (planned.length === 0 && used + required > options.width) {
            const available = options.width - used - column_overhead;
            const width = Math.max(available, 1);
            if (width < entry.width) truncated = true;
            planned.push({column: entry.column, width});
            used += width + column_overhead;
            continue;
        }
        planned.push(entry);
        used += required;
    }

    return {columns: planned, dropped, truncated};
}

export function cell_text(value: unknown, width: number): string {
    return truncate_to_width(format_value(value), width);
}
