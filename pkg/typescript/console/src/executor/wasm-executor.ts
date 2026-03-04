// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult } from '../types';

export interface WasmDB {
  admin(rql: string): Promise<unknown> | unknown;
}

export class WasmExecutor implements Executor {
  private db: WasmDB;

  constructor(db: WasmDB) {
    this.db = db;
  }

  async execute(statement: string): Promise<ExecutionResult> {
    const trimmed = statement.trim();
    const query = trimmed.endsWith(';') ? trimmed.slice(0, -1).trim() : trimmed;

    if (!query) {
      return { success: true, data: [], executionTime: 0 };
    }

    const startTime = performance.now();
    try {
      const results = await this.db.admin(query);
      const executionTime = Math.round(performance.now() - startTime);
      return {
        success: true,
        data: Array.isArray(results) ? results : [],
        executionTime,
      };
    } catch (error) {
      const executionTime = Math.round(performance.now() - startTime);
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        executionTime,
      };
    }
  }

}
