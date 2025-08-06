/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type} from "./types";
import {Interval} from "./interval";
import {ReifyValue} from "./value";

const UNDEFINED_VALUE = "⟪undefined⟫";

export function decodeValue(type: Type, value: string): ReifyValue {
    if (value == UNDEFINED_VALUE) {
        return undefined
    }
    switch (type) {
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
        case "Date":
            // Parse YYYY-MM-DD format from Rust
            // The value might already be in ISO format if it came from JS, 
            // or it might be just YYYY-MM-DD from Rust
            if (value.length === 10 && value[4] === '-' && value[7] === '-') {
                // Just date, add time component for JavaScript Date
                return new Date(value + 'T00:00:00.000Z');
            }
            return new Date(value);
        case "DateTime":
            // Parse YYYY-MM-DDTHH:MM:SS[.nnnnnnnnn]Z format from Rust
            // The Rust side outputs with nanosecond precision
            // JavaScript Date constructor can handle various ISO formats
            return new Date(value);
        case "Time":
            // Parse time string HH:MM:SS[.nnnnnnnnn] from Rust
            // Return as Date with today's date in UTC
            const timeMatch = value.match(/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?/);
            if (!timeMatch) {
                throw new Error(`Invalid time format: ${value}`);
            }
            const [, hoursStr, minutesStr, secondsStr, nanosStr] = timeMatch;
            const hours = parseInt(hoursStr, 10);
            const minutes = parseInt(minutesStr, 10);
            const seconds = parseInt(secondsStr, 10);

            // Create a date in UTC for consistency
            const timeDate = new Date();
            timeDate.setUTCHours(hours, minutes, seconds, 0);

            if (nanosStr) {
                // Convert nanoseconds to milliseconds
                // Pad to 9 digits, then take first 3 for milliseconds
                const paddedNanos = nanosStr.padEnd(9, '0');
                const millis = Math.floor(parseInt(paddedNanos.substring(0, 3), 10));
                timeDate.setUTCMilliseconds(millis);
            }
            return timeDate;
        case "Interval":
            return Interval.parse(value);
        case "Uuid4":
        case "Uuid7":
            return value;
        case "Undefined":
            return undefined;
        default:
            throw new Error(`Unknown data type: ${type}`);
    }
}

