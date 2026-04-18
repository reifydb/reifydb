// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Executor, ExecutionResult, Diagnostic } from '../types';
import { ReifyError } from '@reifydb/core';

export type TransactionType = 'admin' | 'query' | 'command';

export interface WsClient {
  admin<const S extends readonly unknown[]>(
    rql: string,
    params: unknown,
    shapes: S
  ): Promise<unknown[][]>;
  query<const S extends readonly unknown[]>(
    rql: string,
    params: unknown,
    shapes: S
  ): Promise<unknown[][]>;
  command<const S extends readonly unknown[]>(
    rql: string,
    params: unknown,
    shapes: S
  ): Promise<unknown[][]>;
}

function normalize_fragment(raw: unknown): Diagnostic['fragment'] {
  if (!raw || typeof raw !== 'object') return undefined;
  const obj = raw as Record<string, unknown>;
  if ('Statement' in obj && obj.Statement && typeof obj.Statement === 'object') {
    const s = obj.Statement as Record<string, unknown>;
    return { text: String(s.text ?? ''), line: s.line as number, column: s.column as number };
  }
  if ('Internal' in obj && obj.Internal && typeof obj.Internal === 'object') {
    const s = obj.Internal as Record<string, unknown>;
    return { text: String(s.text ?? '') };
  }
  if ('text' in obj) {
    return { text: String(obj.text), line: obj.line as number | undefined, column: obj.column as number | undefined };
  }
  return undefined;
}

function to_diagnostic(error: ReifyError): Diagnostic {
  return {
    code: error.code,
    statement: error.statement,
    message: error.message.replace(/^\[.*?\]\s*/, ''),
    fragment: normalize_fragment(error.fragment),
    label: error.label,
    help: error.help,
    notes: error.notes,
    cause: error.cause,
  };
}

export class WsExecutor implements Executor {
  private client: WsClient;
  transaction_type: TransactionType = 'admin';

  constructor(client: WsClient) {
    this.client = client;
  }

  async execute(statement: string): Promise<ExecutionResult> {
    const trimmed = statement.trim();
    const query = trimmed.endsWith(';') ? trimmed.slice(0, -1).trim() : trimmed;

    if (!query) {
      return { success: true, data: [], execution_time: 0 };
    }

    const start_time = performance.now();
    try {
      const frames = await this.client[this.transaction_type](query, null, []);
      const execution_time = Math.round(performance.now() - start_time);
      const results = frames[0] ?? [];

      const data = results.map((row: unknown) => {
        if (row && typeof row === 'object') {
          const plain_row: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row as Record<string, unknown>)) {
            plain_row[key] = value;
          }
          return plain_row;
        }
        return row as Record<string, unknown>;
      });

      return { success: true, data, execution_time };
    } catch (error) {
      const execution_time = Math.round(performance.now() - start_time);
      if (error instanceof ReifyError) {
        return { success: false, error: error.message, diagnostic: to_diagnostic(error), execution_time };
      }
      const error_message = error instanceof Error ? error.message : String(error);
      return { success: false, error: error_message, execution_time };
    }
  }

}
