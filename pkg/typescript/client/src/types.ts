/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Params, Frame } from "@reifydb/core";

// Re-export common types from core
export {
    Params,
    Frame,
    DiagnosticColumn,
    Span,
    Diagnostic,
    Column,
    ErrorResponse,
    ReifyError
} from "@reifydb/core";

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