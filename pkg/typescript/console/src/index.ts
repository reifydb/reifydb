// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Components
export { Console, type ConsoleProps, type ConnectionConfig, type RdbTheme } from './components/Console';
export { Snippet, type SnippetProps } from './components/Snippet';

// Executors
export { WasmExecutor, type WasmDB } from './executor/wasm-executor';
export { WsExecutor, type WsClient } from './executor/ws-executor';

// Types
export type { Executor, ExecutionResult, HistoryEntry, TransactionType } from './types';

// Monaco assets
export { rqlLanguageDefinition, rqlLanguageConfiguration } from './monaco/rql-language';
export { premiumDarkTheme, premiumLightTheme, brutalistDarkTheme, brutalistLightTheme } from './monaco/themes';
export { registerRqlLanguage } from './monaco/register';
export type { editor } from 'monaco-editor';

// Format utilities
export { formatValue, getValueStyle } from './format/value';
