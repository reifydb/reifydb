// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Components
export { Console, type ConsoleProps, type ConnectionConfig, type RdbTheme } from './components/console';
export { Snippet, type SnippetProps } from './components/snippet';

// Executors
export { WasmExecutor, type WasmDB } from './executor/wasm-executor';
export { WsExecutor, type WsClient } from './executor/ws-executor';

// Types
export type { Executor, ExecutionResult, HistoryEntry, TransactionType } from './types';

// Monaco assets
export { rql_language_definition, rql_language_configuration } from './monaco/rql-language';
export { premium_dark_theme, premium_light_theme, brutalist_dark_theme, brutalist_light_theme } from './monaco/themes';
export { register_rql_language } from './monaco/register';
export type { editor } from 'monaco-editor';

// Format utilities
export { format_value, get_value_style } from './format/value';
