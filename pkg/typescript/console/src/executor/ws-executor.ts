// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult } from '../types';

export interface WsClient {
  admin<const S extends readonly unknown[]>(
    statements: string | string[],
    params: unknown,
    schemas: S
  ): Promise<unknown[][]>;
}

export class WsExecutor implements Executor {
  private client: WsClient;

  constructor(client: WsClient) {
    this.client = client;
  }

  async execute(statement: string): Promise<ExecutionResult> {
    const trimmed = statement.trim();
    const query = trimmed.endsWith(';') ? trimmed.slice(0, -1).trim() : trimmed;

    if (!query) {
      return { success: true, data: [], executionTime: 0 };
    }

    const startTime = performance.now();
    try {
      const frames = await this.client.admin(query, null, []);
      const executionTime = Math.round(performance.now() - startTime);
      const results = frames[0] ?? [];

      const data = results.map((row: unknown) => {
        if (row && typeof row === 'object') {
          const plainRow: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row as Record<string, unknown>)) {
            plainRow[key] = value;
          }
          return plainRow;
        }
        return row as Record<string, unknown>;
      });

      return { success: true, data, executionTime };
    } catch (error) {
      const executionTime = Math.round(performance.now() - startTime);
      let errorMessage: string;
      if (error && typeof error === 'object' && 'diagnostic' in error) {
        const diagnostic = (error as { diagnostic: { message: string } }).diagnostic;
        errorMessage = diagnostic.message;
      } else if (error instanceof Error) {
        errorMessage = error.message;
      } else {
        errorMessage = String(error);
      }
      return { success: false, error: errorMessage, executionTime };
    }
  }

}
