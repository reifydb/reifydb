/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, TypeValuePair} from './value';
import {SchemaNode, InferSchema} from './schema';

export type Params = (TypeValuePair | null)[] | Record<string, TypeValuePair | null>;

export interface Frame {
    columns: Column[];
}

export interface DiagnosticColumn {
    name: string,
    type: Type,
}

export interface Fragment {
    text: string
    line?: number,
    column?: number,
}

export interface Diagnostic {
    code: string,
    statement?: string;
    message: string,
    column?: DiagnosticColumn,
    fragment?: Fragment,
    label?: string,
    help?: string,
    notes: Array<string>,
    cause?: Diagnostic,
}

export interface Column {
    name: string;
    type: Type;
    data: string[];
}

export interface ErrorResponse {
    id: string;
    type: "Err";
    payload: {
        diagnostic: Diagnostic;
    };
}

export class ReifyError extends Error {
    public readonly code: string;
    public readonly statement?: string;
    public readonly column?: DiagnosticColumn;
    public readonly fragment?: Fragment;
    public readonly label?: string;
    public readonly help?: string;
    public readonly notes: string[];
    public readonly cause?: Diagnostic;

    constructor(response: ErrorResponse) {
        const diagnostic = response.payload.diagnostic;
        const message = `[${diagnostic.code}] ${diagnostic.message}` +
            (diagnostic.label ? ` — ${diagnostic.label}` : "");

        super(message);

        this.name = "ReifyError";
        this.code = diagnostic.code;
        this.statement = diagnostic.statement;
        this.column = diagnostic.column;
        this.fragment = diagnostic.fragment;
        this.label = diagnostic.label;
        this.help = diagnostic.help;
        this.notes = diagnostic.notes ?? [];
        this.cause = diagnostic.cause;

        // Required for instanceof checks to work properly
        Object.setPrototypeOf(this, new.target.prototype);
    }

    toString(): string {
        const position = this.fragment
            ? `line ${this.fragment.line}, offset ${this.fragment.column}`
            : "unknown position";

        const notes = this.notes.length
            ? `\nNotes:\n- ${this.notes.join("\n- ")}`
            : "";

        const help = this.help
            ? `\nHelp: ${this.help}`
            : "";

        return `${this.name}: ${this.message}\nAt ${position}${help}${notes}`;
    }
}

/**
 * Simplified type for inferring frame results to avoid deep instantiation
 * This provides a workaround for TypeScript's limitation with deeply nested types
 */
export type FrameResults<S extends readonly SchemaNode[]> =
    S extends readonly [infer First, ...infer Rest]
        ? First extends SchemaNode
            ? Rest extends readonly SchemaNode[]
                ? [InferSchema<First>[], ...FrameResults<Rest>]
                : [InferSchema<First>[]]
            : never
        : [];

/**
 * Helper type to extract a single frame result
 */
export type SingleFrameResult<S extends SchemaNode> = InferSchema<S>[];

/**
 * Type-safe cast helper for frame results
 */
export function asFrameResults<S extends readonly SchemaNode[]>(
    results: any
): FrameResults<S> {
    return results as FrameResults<S>;
}