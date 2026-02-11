// SPDX-License-Identifier: AGPL-3.0-or-later
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
      this.formatError(result.error ?? 'Unknown error', result.executionTime);
      return;
    }

    if (!result.data || result.data.length === 0) {
      this.terminal.writeln('');
      this.terminal.writeln(`${C.dim}Query executed successfully. No rows returned.${C.reset}`);
      this.formatExecutionTime(result.executionTime);
      return;
    }

    this.formatTable(result.data, result.executionTime);
  }

  private formatTable(data: Record<string, unknown>[], executionTime: number): void {
    const renderer = new TableRenderer(data, {
      maxWidth: this.displayMode === 'truncate' ? this.terminal.cols - 2 : undefined,
      truncate: this.displayMode === 'truncate',
    });

    const lines = renderer.render();
    this.terminal.writeln('');
    for (const line of lines) {
      this.terminal.writeln(line);
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
