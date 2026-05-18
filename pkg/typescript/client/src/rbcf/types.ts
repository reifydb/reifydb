// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Type } from "@reifydb/core";

/**
 * A frame decoded from (or ready to encode to) RBCF bytes.
 * Shape matches the JSON-over-WS frame the existing client already consumes,
 * so columns_to_rows + @reifydb/core's decode() work unchanged downstream.
 */
export interface WireColumn {
    name: string;
    type: Type;
    payload: string[];
}

export interface WireFrame {
    columns: WireColumn[];
    // u64 row numbers stringified to avoid JS number precision loss (optional).
    row_numbers?: string[];
    // DateTime ISO-8601 strings (optional).
    created_at?: string[];
    updated_at?: string[];
}
