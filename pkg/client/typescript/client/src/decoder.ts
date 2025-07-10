import {Kind, WebsocketColumn} from "./types";

const UNDEFINED_VALUE = "⟪undefined⟫";

export function decodeValue(kind: Kind, value: string): unknown {
    if (value == UNDEFINED_VALUE) {
        return undefined
    }
    switch (kind) {
        case "Bool":
            return value === "true";
        case "Float4":
        case "Float8":
        case "Int1":
        case "Int2":
        case "Int4":
        case "Uint1":
        case "Uint2":
        case "Uint4":
            return Number(value);
        case "Int8":
        case "Int16":
        case "Uint8":
        case "Uint16":
            return BigInt(value);
        case "Utf8":
            return value;
        case "Undefined":
            return undefined;
        default:
            throw new Error(`Unknown kind: ${kind}`);
    }
}

export function columnsToRows(columns: WebsocketColumn[]): Record<string, unknown>[] {
    const rowCount = columns[0]?.data.length ?? 0;
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, unknown> = {};
        for (const col of columns) {
            row[col.name] = decodeValue(col.kind, col.data[i]);
        }
        return row;
    });
}
