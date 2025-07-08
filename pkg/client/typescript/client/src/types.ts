export type Kind =
    | "Bool" | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4"
    | "String" | "Undefined";

export interface WebsocketFrame {
    columns: WebsocketColumn[];
}

export interface DiagnosticColumn {
    name: string,
    value: Kind,
}

export interface Span {
    offset: number,
    line: number,
    fragment: string
}

export interface Diagnostic {
    code: string,
    message: string,
    column: DiagnosticColumn,
    span: Span,
    label: string,
    help: string,
    notes: Array<string>,
}

export interface WebsocketColumn {
    name: string;
    kind: Kind;
    data: string[];
}

export interface ErrorResponse {
    id: string;
    type: "Error";
    payload: {
        diagnostic: Diagnostic;
    };
}


export interface ExecuteResponse {
    id: string;
    type: "Execute";
    payload: {
        frames: WebsocketFrame[];
    };
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
    public readonly column: DiagnosticColumn;
    public readonly span: Span;
    public readonly label: string;
    public readonly help: string;
    public readonly notes: string[];

    constructor(response: ErrorResponse) {
        const {payload: {diagnostic}} = response;
        const message = `[${diagnostic.code}] ${diagnostic.message} — ${diagnostic.label}`;

        super(message);

        this.name = "ReifyError";
        this.code = diagnostic.code;
        this.column = diagnostic.column;
        this.span = diagnostic.span;
        this.label = diagnostic.label;
        this.help = diagnostic.help;
        this.notes = diagnostic.notes;

        Object.setPrototypeOf(this, ReifyError.prototype);
    }

    toString(): string {
        const position = `line ${this.span.line}, offset ${this.span.offset}`;
        const notes = this.notes.length ? `\nNotes:\n- ${this.notes.join("\n- ")}` : "";
        const help = this.help ? `\nHelp: ${this.help}` : "";
        return `${this.name}: ${this.message}\nAt ${position}${help}${notes}`;
    }
}