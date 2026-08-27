// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export { Console, type ConsoleProps, type ConnectionConfig, type RdbTheme } from './components/console';
export { Snippet, type SnippetProps } from './components/snippet';

export { WasmExecutor, type WasmDB } from './executor/wasm-executor';
export { WsExecutor, type WsClient } from './executor/ws-executor';

export type { Executor, ExecutionResult, HistoryEntry, TransactionType } from './types';

export { rqlLanguageDefinition, rqlLanguageConfiguration } from './monaco/rql-language';
export { premiumDarkTheme, premiumLightTheme, brutalistDarkTheme, brutalistLightTheme } from './monaco/themes';
export { registerRqlLanguage } from './monaco/register';
export type { editor } from 'monaco-editor';

export { formatValue, getValueStyle } from './format/value';
