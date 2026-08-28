// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { TerminalAdapter } from '../terminal/adapter';
import { TableRenderer, type TableBorder, type TableTheme } from './table';
import type { ExecutionResult, DisplayMode } from '../types';

const C = TerminalAdapter.COLORS;

export interface FormatterOptions {
  border?: TableBorder;
  theme?: TableTheme;
}

export class OutputFormatter {
  private terminal: TerminalAdapter;
  private displayMode: DisplayMode;
  private border: TableBorder | undefined;
  private theme: TableTheme | undefined;

  constructor(
    terminal: TerminalAdapter,
    displayMode: DisplayMode = 'full',
    options: FormatterOptions = {}
  ) {
    this.terminal = terminal;
    this.displayMode = displayMode;
    this.border = options.border;
    this.theme = options.theme;
  }

  setDisplayMode(mode: DisplayMode): void {
    this.displayMode = mode;
  }

  formatResult(result: ExecutionResult): void {
    if (!result.success) {
      this.formatError(result.error ?? 'Unknown error', result.executionTime);
      return;
    }

    if (!result.data || result.data.length === 0) {
      this.terminal.writeln('');
      this.terminal.writeln(`${C.dim}Query executed successfully. No rows returned.${C.reset}`);
      this.formatExecutionTime(result.executionTime);
      return;
    }

    this.formatTable(result, result.executionTime);
  }

  private formatTable(result: ExecutionResult, executionTime: number): void {
    const data = result.data ?? [];
    const renderer = new TableRenderer(data, {
      maxWidth: this.displayMode === 'truncate' ? this.terminal.cols - 2 : undefined,
      truncate: this.displayMode === 'truncate',
      border: this.border,
      theme: this.theme,
      columns: result.columns,
    });

    this.terminal.writeln('');
    for (const line of renderer.render()) {
      this.terminal.writeln(line);
    }

    const dropped = renderer.dropped;
    if (dropped.length > 0) {
      this.terminal.writeln(
        `${C.dim}${dropped.length} more column${dropped.length !== 1 ? 's' : ''}: ${dropped.join(', ')}${C.reset}`
      );
    }

    const rowCount = data.length;
    this.terminal.writeln('');
    this.terminal.write(
      `${C.green}${rowCount} row${rowCount !== 1 ? 's' : ''}${C.reset}`
    );
    this.formatExecutionTime(executionTime);
  }

  private formatError(error: string, executionTime: number): void {
    this.terminal.writeln('');
    for (const line of error.split('\n')) {
      this.terminal.writeln(line);
    }
    this.formatExecutionTime(executionTime);
  }

  private formatExecutionTime(ms: number): void {
    this.terminal.writeln(` ${C.dim}(${ms}ms)${C.reset}`);
  }
}
