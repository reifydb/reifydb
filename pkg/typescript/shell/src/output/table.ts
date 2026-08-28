// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  NONE_PRESENTATION,
  cellText,
  inferColumns,
  padToWidth,
  planTable,
  valueRole,
  type PlannedColumn,
  type ResultColumn,
  type TablePlan,
  type ValueRole,
} from '@reifydb/core';
import {paint, sgr, type AnsiStyle} from '../ansi';

export type TableBorder = 'unicode' | 'ascii' | 'none';

export type ValuePalette = Partial<Record<ValueRole, AnsiStyle>>;

export interface TableTheme {
  border?: AnsiStyle;
  header?: AnsiStyle;
  values?: ValuePalette;
}

export interface TableColumn {
  name: string;
  width: number;
}

export interface TableOptions {
  maxWidth?: number;
  truncate?: boolean;
  border?: TableBorder;
  theme?: TableTheme;
  columns?: ResultColumn[];
}

interface BorderGlyphs {
  vertical: string;
  horizontal: string;
  top: [string, string, string];
  middle: [string, string, string];
  bottom: [string, string, string];
  padding: number;
  columnOverhead: number;
  tableOverhead: number;
}

const BORDERS: Record<TableBorder, BorderGlyphs> = {
  unicode: {
    vertical: '│',
    horizontal: '─',
    top: ['┌', '┬', '┐'],
    middle: ['├', '┼', '┤'],
    bottom: ['└', '┴', '┘'],
    padding: 1,
    columnOverhead: 3,
    tableOverhead: 1,
  },
  ascii: {
    vertical: '|',
    horizontal: '-',
    top: ['+', '+', '+'],
    middle: ['+', '+', '+'],
    bottom: ['+', '+', '+'],
    padding: 1,
    columnOverhead: 3,
    tableOverhead: 1,
  },
  none: {
    vertical: '',
    horizontal: '─',
    top: ['', '', ''],
    middle: ['', '', ''],
    bottom: ['', '', ''],
    padding: 1,
    columnOverhead: 2,
    tableOverhead: 0,
  },
};

export const reifydbDarkTableTheme: TableTheme = {
  border: {color: '#3f3f46'},
  header: {color: '#818cf8', bold: true},
  values: {
    none: {
      color: NONE_PRESENTATION.muted ? '#52525b' : undefined,
      italic: NONE_PRESENTATION.italic,
    },
    boolean: {color: '#fbbf24'},
    number: {color: '#a5b4fc'},
    temporal: {color: '#5eead4'},
    uuid: {color: '#c4b5fd'},
    string: {color: '#e4e4e7'},
    blob: {color: '#a1a1aa'},
  },
};

export class TableRenderer {
  private data: Record<string, unknown>[];
  private glyphs: BorderGlyphs;
  private plan: TablePlan;
  private borderPrefix: string;
  private headerPrefix: string;
  private valuePrefixes: Map<ValueRole, string>;

  constructor(data: Record<string, unknown>[], options: TableOptions = {}) {
    this.data = data;
    this.glyphs = BORDERS[options.border ?? 'unicode'];

    const theme = options.theme ?? reifydbDarkTableTheme;
    this.borderPrefix = theme.border ? sgr(theme.border) : '';
    this.headerPrefix = theme.header ? sgr(theme.header) : '';
    this.valuePrefixes = new Map();
    for (const [role, style] of Object.entries(theme.values ?? {})) {
      if (style) this.valuePrefixes.set(role as ValueRole, sgr(style));
    }

    const columns = options.columns ?? inferColumns(data);
    this.plan = planTable(data, columns, {
      width: options.truncate ? options.maxWidth : undefined,
      columnOverhead: this.glyphs.columnOverhead,
      tableOverhead: this.glyphs.tableOverhead,
    });
  }

  get columns(): TableColumn[] {
    return this.plan.columns.map((entry) => ({name: entry.column.name, width: entry.width}));
  }

  get dropped(): string[] {
    return this.plan.dropped.map((column) => column.name);
  }

  render(): string[] {
    if (this.data.length === 0 || this.plan.columns.length === 0) {
      return ['(no results)'];
    }

    const bordered = this.glyphs.vertical !== '';
    const lines: string[] = [];

    if (bordered) lines.push(this.rule(this.glyphs.top));
    lines.push(this.headerRow());
    lines.push(this.rule(this.glyphs.middle));

    for (const row of this.data) {
      lines.push(this.dataRow(row));
    }

    if (bordered) lines.push(this.rule(this.glyphs.bottom));

    return lines;
  }

  private rule(corners: [string, string, string]): string {
    const pad = this.glyphs.padding * 2;

    if (this.glyphs.vertical === '') {
      return paint(this.glyphs.horizontal.repeat(this.width()), this.borderPrefix);
    }

    const segments = this.plan.columns.map((entry) =>
      this.glyphs.horizontal.repeat(entry.width + pad)
    );
    const [left, joint, right] = corners;
    return paint(left + segments.join(joint) + right, this.borderPrefix);
  }

  private width(): number {
    const gaps = this.glyphs.columnOverhead * (this.plan.columns.length - 1);
    const body = this.plan.columns.reduce((total, entry) => total + entry.width, 0);
    return this.glyphs.padding + body + gaps;
  }

  private headerRow(): string {
    const cells = this.plan.columns.map((entry) =>
      paint(padToWidth(entry.column.name, entry.width, entry.column.align), this.headerPrefix)
    );
    return this.joinCells(cells);
  }

  private dataRow(row: Record<string, unknown>): string {
    const cells = this.plan.columns.map((entry) => {
      const value = row[entry.column.name];
      const text = padToWidth(cellText(value, entry.width), entry.width, entry.column.align);
      return paint(text, this.valuePrefixes.get(valueRole(value)) ?? '');
    });
    return this.joinCells(cells);
  }

  private joinCells(cells: string[]): string {
    const pad = ' '.repeat(this.glyphs.padding);

    if (this.glyphs.vertical === '') {
      const gap = ' '.repeat(this.glyphs.columnOverhead);
      return pad + cells.join(gap);
    }

    const bar = paint(this.glyphs.vertical, this.borderPrefix);
    return bar + pad + cells.join(pad + bar + pad) + pad + bar;
  }
}

export type {PlannedColumn, ResultColumn};
