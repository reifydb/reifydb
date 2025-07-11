/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {DataType} from "./types";

const UNDEFINED_VALUE = "⟪undefined⟫";

export function decodeValue(data_type: DataType, value: string): unknown {
    if (value == UNDEFINED_VALUE) {
        return undefined
    }
    switch (data_type) {
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
            throw new Error(`Unknown data type: ${data_type}`);
    }
}

