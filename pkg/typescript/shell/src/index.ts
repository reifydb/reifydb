// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export { Shell } from './shell';

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

export { TerminalAdapter } from './terminal/adapter';
export type { KeyHandler } from './terminal/adapter';
export { defaultTheme, reifydbDarkTheme, COLORS } from './terminal/theme';
export type { TerminalTheme } from './terminal/theme';

export { LineEditor } from './input/line-editor';
export { CommandHistory, LocalStorageHistoryStorage, MemoryHistoryStorage } from './input/history';
export { MultilineBuffer } from './input/multiline';
export { rqlHighlighter, reifydbDarkPalette } from './input/highlight';
export type { RqlPalette } from './input/highlight';
export { sgr, paint, RESET } from './ansi';
export type { AnsiStyle } from './ansi';

export { OutputFormatter } from './output/formatter';
export type { FormatterOptions } from './output/formatter';
export { TableRenderer, reifydbDarkTableTheme } from './output/table';
export type {
  TableBorder,
  TableColumn,
  TableOptions,
  TableTheme,
  ValuePalette,
} from './output/table';

export { WasmExecutor, WsExecutor } from './executors';
export type { WasmDB, WsClient } from './executors';

export { handleDotCommand } from './commands/dot-commands';
