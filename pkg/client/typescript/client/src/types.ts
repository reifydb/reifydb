export type Kind =
    | "Bool" | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4"
    | "String" | "Undefined";

export interface WebsocketFrame {
    columns: WebsocketColumn[];
}

export interface WebsocketColumn {
    name: string;
    kind: Kind;
    data: string[];
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

