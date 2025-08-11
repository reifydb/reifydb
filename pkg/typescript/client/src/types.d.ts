/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import { Type, TypeValuePair } from "@reifydb/core";
export type Params = (TypeValuePair | null)[] | Record<string, TypeValuePair | null>;
export interface Frame {
    columns: Column[];
}
export interface DiagnosticColumn {
    name: string;
    ty: Type;
}
export interface Span {
    offset: number;
    line: number;
    fragment: string;
}
export interface Diagnostic {
    code: string;
    statement?: string;
    message: string;
    column?: DiagnosticColumn;
    span?: Span;
    label?: string;
    help?: string;
    notes: Array<string>;
    cause?: Diagnostic;
}
export interface Column {
    name: string;
    ty: Type;
    data: string[];
}
export interface ErrorResponse {
    id: string;
    type: "Err";
    payload: {
        diagnostic: Diagnostic;
    };
}
export interface CommandRequest {
    id: string;
    type: "Command";
    payload: {
        statements: string[];
        params?: Params;
    };
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
    };
}
export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        frames: Frame[];
    };
}
export declare class ReifyError extends Error {
    readonly code: string;
    readonly statement?: string;
    readonly column?: DiagnosticColumn;
    readonly span?: Span;
    readonly label?: string;
    readonly help?: string;
    readonly notes: string[];
    readonly cause?: Diagnostic;
    constructor(response: ErrorResponse);
    toString(): string;
}
