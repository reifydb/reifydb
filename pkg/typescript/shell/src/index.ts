// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Main Shell class
export { Shell } from './shell';

// Core types
export type {
  Executor,
  ExecutionResult,
  ShellOptions,
  DisplayMode,
  HistoryStorage,
  DotCommandContext,
  DotCommandResult,
  Highlighter,
} from './types';

// Terminal components
export { TerminalAdapter } from './terminal/adapter';
export type { KeyHandler } from './terminal/adapter';
export { defaultTheme, reifydb_dark_theme, COLORS } from './terminal/theme';
export type { TerminalTheme } from './terminal/theme';

// Input components
export { LineEditor } from './input/line-editor';
export { CommandHistory, LocalStorageHistoryStorage, MemoryHistoryStorage } from './input/history';
export { MultilineBuffer } from './input/multiline';
export { rql_highlighter, reifydb_dark_palette } from './input/highlight';
export type { RqlPalette } from './input/highlight';
export { sgr, paint, RESET } from './ansi';
export type { AnsiStyle } from './ansi';

// Output components
export { OutputFormatter } from './output/formatter';
export type { FormatterOptions } from './output/formatter';
export { TableRenderer, reifydb_dark_table_theme } from './output/table';
export type {
  TableBorder,
  TableColumn,
  TableOptions,
  TableTheme,
  ValuePalette,
} from './output/table';

// Executors
export { WasmExecutor, WsExecutor } from './executors';
export type { WasmDB, WsClient } from './executors';

// Commands
export { handleDotCommand } from './commands/dot-commands';
