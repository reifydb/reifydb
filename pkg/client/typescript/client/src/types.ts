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
    statement?: string;
    message: string,
    column?: DiagnosticColumn,
    span?: Span,
    label?: string,
    help?: string,
    notes: Array<string>,
}

export interface WebsocketColumn {
    name: string;
    kind: Kind;
    data: string[];
}

export interface ErrorResponse {
    id: string;
    type: "Err";
    payload: {
        diagnostic: Diagnostic;
    };
}


export interface TxResponse {
    id: string;
    type: "Tx";
    payload: {
        frames: WebsocketFrame[];
    };
}

export interface RxResponse {
    id: string;
    type: "Rx";
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