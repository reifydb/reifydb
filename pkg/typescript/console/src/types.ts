// SPDX-License-Identifier: AGPL-3.0-or-later
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
}

export interface HistoryEntry {
  id: string;
  query: string;
  timestamp: number;
  success: boolean;
  row_count?: number;
  execution_time: number;
}
