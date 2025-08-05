/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

export type DataType =
    | "Bool"
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Utf8"
    | "Date" | "DateTime" | "Time" | "Interval"
    | "Uuid4" | "Uuid7"
    | "Undefined";

export interface WebsocketFrame {
    columns: WebsocketColumn[];
}

export interface DiagnosticColumn {
    name: string,
    ty: DataType,
}

export interface Span {
    offset: number,
    line: number,
    fragment: string
}

export interface Diagnostic {
    code: string,
    statement?: string;
    message: string,
    column?: DiagnosticColumn,
    span?: Span,
    label?: string,
    help?: string,
    notes: Array<string>,
    cause?: Diagnostic,
}

export interface WebsocketColumn {
    name: string;
    ty: DataType;
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
    }
}

export interface CommandResponse {
    id: string;
    type: "Command";
    payload: {
        frames: WebsocketFrame[];
    };
}

export interface QueryRequest {
    id: string;
    type: "Query";
    payload: {
        statements: string[];
    }
}

export interface QueryResponse {
    id: string;
    type: "Query";
    payload: {
        frames: WebsocketFrame[];
    };
}


export class ReifyError extends Error {
    public readonly code: string;
    public readonly statement?: string;
    public readonly column?: DiagnosticColumn;
    public readonly span?: Span;
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
        this.span = diagnostic.span;
        this.label = diagnostic.label;
        this.help = diagnostic.help;
        this.notes = diagnostic.notes ?? [];
        this.cause = diagnostic.cause;

        // Required for instanceof checks to work properly
        Object.setPrototypeOf(this, new.target.prototype);
    }

    toString(): string {
        const position = this.span
            ? `line ${this.span.line}, offset ${this.span.offset}`
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