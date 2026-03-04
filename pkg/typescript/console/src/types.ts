// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

export interface ExecutionResult {
  success: boolean;
  data?: Record<string, unknown>[];
  error?: string;
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
