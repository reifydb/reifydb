// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
} from './types';

// Terminal components
export { TerminalAdapter } from './terminal/adapter';
export type { KeyHandler } from './terminal/adapter';
export { defaultTheme, COLORS } from './terminal/theme';
export type { TerminalTheme } from './terminal/theme';

// Input components
export { LineEditor } from './input/line-editor';
export { CommandHistory, LocalStorageHistoryStorage, MemoryHistoryStorage } from './input/history';
export { MultilineBuffer } from './input/multiline';

// Output components
export { OutputFormatter } from './output/formatter';
export { TableRenderer } from './output/table';
export type { TableColumn, TableOptions } from './output/table';

// Executors
export { WasmExecutor, WsExecutor } from './executors';
export type { WasmDB, WsClient } from './executors';

// Commands
export { handleDotCommand } from './commands/dot-commands';
