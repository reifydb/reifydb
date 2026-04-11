// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult } from '../types';

/**
 * Interface for WebAssembly database instances.
 * This matches the WasmDB interface from reifydb-wasm.
 */
export interface WasmDB {
  admin(rql: string): Promise<unknown> | unknown;
}

/**
 * Executor adapter for WebAssembly-based ReifyDB instances.
 */
export class WasmExecutor implements Executor {
  private db: WasmDB;

  constructor(db: WasmDB) {
    this.db = db;
  }

  async execute(statement: string): Promise<ExecutionResult> {
    const trimmed = statement.trim();

    // Remove trailing semicolon for execution
    const query = trimmed.endsWith(';')
      ? trimmed.slice(0, -1).trim()
      : trimmed;

    if (!query) {
      return {
        success: true,
        data: [],
        execution_time: 0,
      };
    }

    const start_time = performance.now();

    try {
      const results = await this.db.admin(query);
      const endTime = performance.now();

      return {
        success: true,
        data: Array.isArray(results) ? results : [],
        execution_time: Math.round(endTime - start_time),
      };
    } catch (error) {
      const endTime = performance.now();

      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        execution_time: Math.round(endTime - start_time),
      };
    }
  }

  async getTables(): Promise<string[]> {
    try {
      // Query system catalog for tables
      const result = await this.db.admin('FROM system::tables MAP { namespace, name }');
      if (Array.isArray(result)) {
        return result.map((row: Record<string, unknown>) => {
          const ns = row.namespace as string;
          const name = row.name as string;
          return ns ? `${ns}::${name}` : name;
        });
      }
      return [];
    } catch {
      return [];
    }
  }

  async getShape(table_name: string): Promise<string | null> {
    try {
      // Query system catalog for table shape
      const result = await this.db.admin(
        `FROM system::columns FILTER table = "${table_name}" MAP { name, type }`
      );
      if (Array.isArray(result) && result.length > 0) {
        const columns = result.map((row: Record<string, unknown>) =>
          `  ${row.name}: ${row.type}`
        ).join(',\n');
        return `${table_name} {\n${columns}\n}`;
      }
      return null;
    } catch {
      return null;
    }
  }
}
