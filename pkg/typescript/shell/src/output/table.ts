// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

export interface TableColumn {
  name: string;
  width: number;
}

export interface TableOptions {
  maxWidth?: number;
  truncate?: boolean;
}

export class TableRenderer {
  private data: Record<string, unknown>[];
  private columns: TableColumn[];
  private maxWidth: number;
  private truncate: boolean;

  constructor(data: Record<string, unknown>[], options: TableOptions = {}) {
    this.data = data;
    this.maxWidth = options.maxWidth ?? 120;
    this.truncate = options.truncate ?? true;
    this.columns = this.calculateColumns();
  }

  private calculateColumns(): TableColumn[] {
    if (this.data.length === 0) return [];

    const columns: Map<string, number> = new Map();

    // Get all column names and calculate max widths
    for (const row of this.data) {
      for (const [key, value] of Object.entries(row)) {
        const valueStr = this.formatValue(value);
        const currentMax = columns.get(key) ?? key.length;
        columns.set(key, Math.max(currentMax, valueStr.length));
      }
    }

    return Array.from(columns.entries()).map(([name, width]) => ({
      name,
      width: Math.max(width, name.length),
    }));
  }

  private formatValue(value: unknown): string {
    if (value === null || value === undefined) {
      return 'null';
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    return String(value);
  }

  private truncateString(str: string, maxLen: number): string {
    if (str.length <= maxLen) return str;
    if (maxLen <= 3) return str.slice(0, maxLen);
    return str.slice(0, maxLen - 3) + '...';
  }

  render(): string[] {
    if (this.data.length === 0 || this.columns.length === 0) {
      return ['(no results)'];
    }

    const lines: string[] = [];
    let columnsToShow = this.columns;
    let widths = this.columns.map((c) => c.width);

    // Calculate how many columns fit
    if (this.truncate) {
      const result = this.fitColumns(this.maxWidth);
      columnsToShow = result.columns;
      widths = result.widths;
    }

    // Build separator line
    const separator =
      '+' + widths.map((w) => '-'.repeat(w + 2)).join('+') + '+';

    lines.push(separator);

    // Build header
    const headerCells = columnsToShow.map((col, i) =>
      this.padCenter(col.name, widths[i])
    );
    lines.push('| ' + headerCells.join(' | ') + ' |');
    lines.push(separator);

    // Build rows
    for (const row of this.data) {
      const cells = columnsToShow.map((col, i) => {
        const value = this.formatValue(row[col.name]);
        const truncated = this.truncate
          ? this.truncateString(value, widths[i])
          : value;
        return this.padRight(truncated, widths[i]);
      });
      lines.push('| ' + cells.join(' | ') + ' |');
    }

    lines.push(separator);

    return lines;
  }

  private fitColumns(maxWidth: number): {
    columns: TableColumn[];
    widths: number[];
  } {
    const columns: TableColumn[] = [];
    const widths: number[] = [];
    let currentWidth = 1; // Starting '|'

    for (const col of this.columns) {
      // Each column: ' content ' + '|' = 3 extra chars
      const colWidth = Math.min(col.width, 40); // Cap individual column width
      const totalColWidth = colWidth + 3;

      if (currentWidth + totalColWidth <= maxWidth) {
        columns.push(col);
        widths.push(colWidth);
        currentWidth += totalColWidth;
      } else {
        break;
      }
    }

    // If no columns fit, show at least the first one
    if (columns.length === 0 && this.columns.length > 0) {
      const firstCol = this.columns[0];
      const availableWidth = maxWidth - 4; // '| ' and ' |'
      columns.push(firstCol);
      widths.push(Math.max(availableWidth, 10));
    }

    return { columns, widths };
  }

  private padCenter(str: string, width: number): string {
    const padding = width - str.length;
    if (padding <= 0) return str.slice(0, width);
    const left = Math.floor(padding / 2);
    const right = padding - left;
    return ' '.repeat(left) + str + ' '.repeat(right);
  }

  private padRight(str: string, width: number): string {
    const padding = width - str.length;
    if (padding <= 0) return str.slice(0, width);
    return str + ' '.repeat(padding);
  }
}
