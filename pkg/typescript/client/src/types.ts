// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
import { ReifyError } from "@reifydb/core";

// Re-export types that are actually available in flow
export type { Params, Frame, Column, ErrorResponse } from "@reifydb/core";
export { ReifyError } from "@reifydb/core";

export interface CommandRequest {
    id: string;
    type: "Command";
    payload: {
        statements: string[];
        params?: Params;
    }
}

export interface CommandResponse {
    id: string;
    type: "Command";
    payload: {
        frames: Frame[];
    };
}

export interface QueryRequest {
    id: string;
    type: "Query";
    payload: {
        statements: string[];
        params?: Params;
    }
}

export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        frames: Frame[];
    };
}