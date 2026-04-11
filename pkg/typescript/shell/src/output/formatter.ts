// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { TerminalAdapter } from '../terminal/adapter';
import { TableRenderer } from './table';
import type { ExecutionResult, DisplayMode } from '../types';

const C = TerminalAdapter.COLORS;

export class OutputFormatter {
  private terminal: TerminalAdapter;
  private displayMode: DisplayMode;

  constructor(terminal: TerminalAdapter, displayMode: DisplayMode = 'full') {
    this.terminal = terminal;
    this.displayMode = displayMode;
  }

  setDisplayMode(mode: DisplayMode): void {
    this.displayMode = mode;
  }

  formatResult(result: ExecutionResult): void {
    if (!result.success) {
      this.formatError(result.error ?? 'Unknown error', result.execution_time);
      return;
    }

    if (!result.data || result.data.length === 0) {
      this.terminal.writeln('');
      this.terminal.writeln(`${C.dim}Query executed successfully. No rows returned.${C.reset}`);
      this.formatExecutionTime(result.execution_time);
      return;
    }

    this.formatTable(result.data, result.execution_time);
  }

  private formatTable(data: Record<string, unknown>[], execution_time: number): void {
    const renderer = new TableRenderer(data, {
      maxWidth: this.displayMode === 'truncate' ? this.terminal.cols - 2 : undefined,
      truncate: this.displayMode === 'truncate',
    });

    const lines = renderer.render();
    this.terminal.writeln('');
    for (const line of lines) {
      this.terminal.writeln(line);
    }

    const row_count = data.length;
    this.terminal.writeln('');
    this.terminal.write(
      `${C.green}${row_count} row${row_count !== 1 ? 's' : ''}${C.reset}`
    );
    this.formatExecutionTime(execution_time);
  }

  private formatError(error: string, execution_time: number): void {
    this.terminal.writeln('');
    for (const line of error.split('\n')) {
      this.terminal.writeln(line);
    }
    this.formatExecutionTime(execution_time);
  }

  private formatExecutionTime(ms: number): void {
    this.terminal.writeln(` ${C.dim}(${ms}ms)${C.reset}`);
  }
}
