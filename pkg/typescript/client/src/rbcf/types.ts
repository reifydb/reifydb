// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Type } from "@reifydb/core";

/**
 * A frame decoded from (or ready to encode to) RBCF bytes.
 * Shape matches the JSON-over-WS frame the existing client already consumes,
 * so columnsToRows + @reifydb/core's decode() work unchanged downstream.
 */
export interface WireColumn {
    name: string;
    type: Type;
    payload: string[];
}

export type WireOp = 1 | 2 | 3;

export interface WireFrame {
    columns: WireColumn[];
    // Change operation for the whole frame: 1=insert, 2=update, 3=remove. Absent on query results.
    op?: WireOp;
    // u64 row numbers stringified to avoid JS number precision loss (optional).
    row_numbers?: string[];
    // DateTime ISO-8601 strings (optional).
    created_at?: string[];
    updated_at?: string[];
}
