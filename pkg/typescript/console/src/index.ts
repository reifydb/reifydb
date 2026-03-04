// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Components
export { Console, type ConsoleProps } from './components/Console';
export { Snippet, type SnippetProps } from './components/Snippet';

// Executors
export { WasmExecutor, type WasmDB } from './executor/wasm-executor';
export { WsExecutor, type WsClient } from './executor/ws-executor';

// Types
export type { Executor, ExecutionResult, HistoryEntry } from './types';

// Monaco assets
export { rqlLanguageDefinition, rqlLanguageConfiguration } from './monaco/rql-language';
export { premiumDarkTheme, brutalistDarkTheme, brutalistLightTheme } from './monaco/themes';
export { registerRqlLanguage } from './monaco/register';

// Format utilities
export { formatValue, getValueStyle } from './format/value';
