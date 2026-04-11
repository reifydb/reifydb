// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult, Diagnostic } from '../types';

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
      return { success: true, data: [], execution_time: 0 };
    }

    const start_time = performance.now();
    try {
      const results = await this.db.admin(query);
      const execution_time = Math.round(performance.now() - start_time);
      return {
        success: true,
        data: Array.isArray(results) ? results : [],
        execution_time,
      };
    } catch (error) {
      const execution_time = Math.round(performance.now() - start_time);
      let diagnostic: Diagnostic | undefined;
      if (error && typeof error === 'object' && 'code' in error) {
        const e = error as Record<string, unknown>;
        diagnostic = {
          code: String(e.code),
          message: String(e.message ?? ''),
          statement: e.statement as string | undefined,
          fragment: e.fragment as Diagnostic['fragment'],
          label: e.label as string | undefined,
          help: e.help as string | undefined,
          notes: Array.isArray(e.notes) ? e.notes.map(String) : [],
          cause: e.cause as Diagnostic | undefined,
        };
      }
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        diagnostic,
        execution_time,
      };
    }
  }

}
