// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
  execute(statement: string): Promise<ExecutionResult>;
}

export interface HistoryEntry {
  id: string;
  query: string;
  timestamp: number;
  success: boolean;
  row_count?: number;
  execution_time: number;
}
