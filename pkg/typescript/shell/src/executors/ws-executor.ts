// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult } from '../types';

/**
 * Interface for WebSocket client.
 * This matches the WsClient interface from @reifydb/client.
 */
export interface WsClient {
  admin<const S extends readonly unknown[]>(
    statements: string | string[],
    params: unknown,
    shapes: S
  ): Promise<unknown[][]>;
}

/**
 * Executor adapter for WebSocket-based ReifyDB connections.
 */
export class WsExecutor implements Executor {
  private client: WsClient;

  constructor(client: WsClient) {
    this.client = client;
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
      // Execute via admin endpoint with no shape transformation
      const frames = await this.client.admin(query, null, []);
      const endTime = performance.now();

      // Get first frame results (admin typically returns single frame)
      const results = frames[0] ?? [];

      // Convert results to plain objects, keeping Value objects as-is
      const data = results.map((row: unknown) => {
        if (row && typeof row === 'object') {
          const plainRow: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row as Record<string, unknown>)) {
            plainRow[key] = value;  // Keep Value objects as-is
          }
          return plainRow;
        }
        return row as Record<string, unknown>;
      });

      return {
        success: true,
        data,
        execution_time: Math.round(endTime - start_time),
      };
    } catch (error) {
      const endTime = performance.now();

      // Extract error message from ReifyError if present
      let error_message: string;
      if (error && typeof error === 'object' && 'diagnostic' in error) {
        const diagnostic = (error as { diagnostic: { message: string } }).diagnostic;
        error_message = diagnostic.message;
      } else if (error instanceof Error) {
        error_message = error.message;
      } else {
        error_message = String(error);
      }

      return {
        success: false,
        error: error_message,
        execution_time: Math.round(endTime - start_time),
      };
    }
  }

  async getTables(): Promise<string[]> {
    try {
      const frames = await this.client.admin(
        'FROM system::tables MAP { namespace, name }',
        null,
        []
      );
      const results = frames[0] ?? [];

      return results.map((row: unknown) => {
        const r = row as Record<string, unknown>;
        const ns = this.extractValue(r.namespace) as string;
        const name = this.extractValue(r.name) as string;
        return ns ? `${ns}::${name}` : name;
      });
    } catch {
      return [];
    }
  }

  async getShape(table_name: string): Promise<string | null> {
    try {
      const frames = await this.client.admin(
        `FROM system::columns FILTER table = "${table_name}" MAP { name, type }`,
        null,
        []
      );
      const results = frames[0] ?? [];

      if (results.length > 0) {
        const columns = results.map((row: unknown) => {
          const r = row as Record<string, unknown>;
          return `  ${this.extractValue(r.name)}: ${this.extractValue(r.type)}`;
        }).join(',\n');
        return `${table_name} {\n${columns}\n}`;
      }
      return null;
    } catch {
      return null;
    }
  }

  private extractValue(value: unknown): unknown {
    if (value && typeof value === 'object' && typeof (value as { valueOf(): unknown }).valueOf === 'function') {
      return (value as { valueOf(): unknown }).valueOf();
    }
    return value;
  }
}
