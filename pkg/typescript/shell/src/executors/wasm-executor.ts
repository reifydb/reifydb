// SPDX-License-Identifier: AGPL-3.0-or-later
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
        executionTime: 0,
      };
    }

    const startTime = performance.now();

    try {
      const results = await this.db.admin(query);
      const endTime = performance.now();

      return {
        success: true,
        data: Array.isArray(results) ? results : [],
        executionTime: Math.round(endTime - startTime),
      };
    } catch (error) {
      const endTime = performance.now();

      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
        executionTime: Math.round(endTime - startTime),
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

  async getSchema(tableName: string): Promise<string | null> {
    try {
      // Query system catalog for table schema
      const result = await this.db.admin(
        `FROM system::columns FILTER table = "${tableName}" MAP { name, type }`
      );
      if (Array.isArray(result) && result.length > 0) {
        const columns = result.map((row: Record<string, unknown>) =>
          `  ${row.name}: ${row.type}`
        ).join(',\n');
        return `${tableName} {\n${columns}\n}`;
      }
      return null;
    } catch {
      return null;
    }
  }
}
