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
  executionTime: number;
}

export interface Executor {
  execute(statement: string): Promise<ExecutionResult>;
}

export interface HistoryEntry {
  id: string;
  query: string;
  timestamp: number;
  success: boolean;
  rowCount?: number;
  executionTime: number;
}
