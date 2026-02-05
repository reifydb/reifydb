// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { TerminalAdapter } from './terminal/adapter';
import type { TerminalTheme } from './terminal/theme';

/**
 * Result from executing a statement
 */
export interface ExecutionResult {
  success: boolean;
  data?: Record<string, unknown>[];
  error?: string;
  executionTime: number;
}

/**
 * Interface for executing database statements.
 * Implement this interface to connect the shell to any database backend.
 */
export interface Executor {
  /**
   * Execute a statement and return the result
   */
  execute(statement: string): Promise<ExecutionResult>;

  /**
   * Get list of tables (optional, used by .tables command)
   */
  getTables?(): Promise<string[]>;

  /**
   * Get schema for a table (optional, used by .schema command)
   */
  getSchema?(tableName: string): Promise<string | null>;
}

/**
 * Display mode for query results
 */
export type DisplayMode = 'truncate' | 'full';

/**
 * Interface for history storage.
 * Implement this to customize where history is persisted.
 */
export interface HistoryStorage {
  load(): string[];
  save(entries: string[]): void;
}

/**
 * Configuration options for the Shell
 */
export interface ShellOptions {
  /**
   * The executor to use for running statements
   */
  executor: Executor;

  /**
   * Welcome message shown when shell starts.
   * Can be a string, array of strings, or function returning strings.
   * If not provided, a default welcome banner is shown.
   */
  welcomeMessage?: string | string[] | (() => string[]);

  /**
   * Primary prompt string (default: "reifydb> ")
   * Can include ANSI color codes.
   */
  prompt?: string;

  /**
   * Prompt length without ANSI codes (for cursor positioning)
   * Required if prompt contains ANSI codes.
   */
  promptLength?: number;

  /**
   * Continuation prompt for multi-line input (default: "     ... ")
   * Can include ANSI color codes.
   */
  continuationPrompt?: string;

  /**
   * Continuation prompt length without ANSI codes
   * Required if continuationPrompt contains ANSI codes.
   */
  continuationPromptLength?: number;

  /**
   * Terminal theme colors
   */
  theme?: TerminalTheme;

  /**
   * Key for localStorage history (default: "reifydb-shell-history")
   */
  historyKey?: string;

  /**
   * Custom history storage implementation.
   * If provided, historyKey is ignored.
   */
  historyStorage?: HistoryStorage;

  /**
   * Initial display mode (default: "full")
   */
  displayMode?: DisplayMode;

  /**
   * Callback when user exits the shell
   */
  onExit?: () => void;

  /**
   * Callback when fullscreen state changes
   */
  onFullscreenChange?: (isFullscreen: boolean) => void;
}

/**
 * Internal context passed to dot command handlers
 */
export interface DotCommandContext {
  terminal: TerminalAdapter;
  executor: Executor;
  history: {
    getAll(): string[];
    clear(): void;
  };
  displayMode: DisplayMode;
  setDisplayMode: (mode: DisplayMode) => void;
  clearScreen: () => void;
  isFullscreen: boolean;
  enterFullscreen: () => void;
  exitFullscreen: () => void;
}

/**
 * Result from handling a dot command
 */
export interface DotCommandResult {
  handled: boolean;
  exit?: boolean;
}
