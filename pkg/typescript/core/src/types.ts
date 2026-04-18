// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {Type, TypeValuePair} from './value';
import {ShapeNode, InferShape} from './shape';

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
    rql?: string;
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
    payload: string[];
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
    public readonly rql?: string;
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
        this.rql = diagnostic.rql;
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
export type FrameResults<S extends readonly ShapeNode[]> =
    S extends readonly [infer First, ...infer Rest]
        ? First extends ShapeNode
            ? Rest extends readonly ShapeNode[]
                ? [InferShape<First>[], ...FrameResults<Rest>]
                : [InferShape<First>[]]
            : never
        : [];

/**
 * Helper type to extract a single frame result
 */
export type SingleFrameResult<S extends ShapeNode> = InferShape<S>[];

/**
 * Type-safe cast helper for frame results
 */
export function asFrameResults<S extends readonly ShapeNode[]>(
    results: any
): FrameResults<S> {
    return results as FrameResults<S>;
}
