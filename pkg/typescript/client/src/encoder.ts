/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {ReifyValue} from "./value";
import {Type, WsValue} from "./types";
import {Interval} from "./interval";

const UNDEFINED_VALUE = "⟪undefined⟫";

export function encodeValue(value: ReifyValue): WsValue | null {
    if (value === null || value === undefined) {
        return null;
    }

    // Handle typed values
    if (typeof value === 'object' && 'type' in value && 'value' in value) {
        return {
            type: value.type as Type,
            value: encodeTypedValue(value.type as Type, value.value)
        };
    }

    // Auto-infer types from primitives and convert to TypedValue
    if (typeof value === 'boolean') {
        return { type: "Bool", value: String(value) };
    }
    
    if (typeof value === 'number') {
        // Default to Float8 for numbers
        return { type: "Float8", value: String(value) };
    }
    
    if (typeof value === 'bigint') {
        return { type: "Int8", value: value.toString() };
    }
    
    if (typeof value === 'string') {
        return { type: "Utf8", value: value };
    }
    
    if (value instanceof Date) {
        return { type: "DateTime", value: value.toISOString() };
    }
    
    if (value instanceof Interval) {
        return { type: "Interval", value: value.toString() };
    }

    // Fallback for unknown types
    return { type: "Utf8", value: String(value) };
}

function encodeTypedValue(type: Type, value: any): string {
    switch (type) {
        case "Bool":
            return String(Boolean(value));
        case "Float4":
        case "Float8":
        case "Int1":
        case "Int2":
        case "Int4":
        case "Uint1":
        case "Uint2":
        case "Uint4":
            return String(Number(value));
        case "Int8":
        case "Int16":
        case "Uint8":
        case "Uint16":
            return typeof value === 'bigint' ? value.toString() : String(value);
        case "Utf8":
        case "Uuid4":
        case "Uuid7":
            return String(value);
        case "Date":
            if (value instanceof Date) {
                // Format as YYYY-MM-DD in UTC (matching Rust parser expectations)
                const year = value.getUTCFullYear().toString().padStart(4, '0');
                const month = (value.getUTCMonth() + 1).toString().padStart(2, '0');
                const day = value.getUTCDate().toString().padStart(2, '0');
                return `${year}-${month}-${day}`;
            }
            return String(value);
        case "DateTime":
            if (value instanceof Date) {
                // Use ISO string format for DateTime
                return value.toISOString();
            }
            return String(value);
        case "Time":
            if (value instanceof Date) {
                // Format as HH:MM:SS.nnnnnnnnn in UTC (matching Rust parser expectations)
                const hours = value.getUTCHours().toString().padStart(2, '0');
                const minutes = value.getUTCMinutes().toString().padStart(2, '0');
                const seconds = value.getUTCSeconds().toString().padStart(2, '0');
                const millis = value.getUTCMilliseconds();
                // Convert milliseconds to nanoseconds (pad to 9 digits)
                const nanos = (millis * 1000000).toString().padStart(9, '0');
                return `${hours}:${minutes}:${seconds}.${nanos}`;
            }
            return String(value);
        case "Interval":
            return value instanceof Interval ? value.toString() : String(value);
        case "Undefined":
            return UNDEFINED_VALUE;
        default:
            return String(value);
    }
}

export function encodeParams(params: ReifyValue[] | Record<string, ReifyValue>): (WsValue | null)[] | Record<string, WsValue | null> {
    if (Array.isArray(params)) {
        return params.map(encodeValue);
    } else {
        const encoded: Record<string, WsValue | null> = {};
        for (const [key, val] of Object.entries(params)) {
            encoded[key] = encodeValue(val);
        }
        return encoded;
    }
}