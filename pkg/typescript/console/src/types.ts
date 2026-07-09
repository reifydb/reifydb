// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Diagnostic } from '@reifydb/core';

export type { Diagnostic };
export type { TransactionType } from './executor/ws-executor';

export interface ExecutionResult {
  success: boolean;
  data?: Record<string, unknown>[];
  error?: string;
  diagnostic?: Diagnostic;
  execution_time: number;
}

export interface Executor {
  execute(rql: string): Promise<ExecutionResult>;
  /** Returns false while the executor still needs to initialize (e.g. a WASM module download) before it can run a query. */
  isReady?(): boolean;
}

export interface HistoryEntry {
  id: string;
  query: string;
  timestamp: number;
  success: boolean;
  row_count?: number;
  execution_time: number;
}
