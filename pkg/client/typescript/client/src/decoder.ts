/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {DataType} from "./types";
import {Interval} from "./interval";

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
        case "Date":
            return new Date(value);
        case "DateTime":
            return new Date(value);
        case "Time":
            // Parse time string (HH:MM:SS.nnnnnnnnn) and return as Date with today's date
            const [time, nanos] = value.split('.');
            const [hours, minutes, seconds] = time.split(':').map(Number);
            const timeDate = new Date();
            timeDate.setHours(hours, minutes, seconds, 0);
            if (nanos) {
                // Add nanoseconds as microseconds (JavaScript Date only supports milliseconds)
                const microseconds = Math.floor(parseInt(nanos.padEnd(9, '0')) / 1000000);
                timeDate.setMilliseconds(microseconds);
            }
            return timeDate;
        case "Interval":
            // Return interval as Interval instance
            return Interval.parse(value);
        case "Undefined":
            return undefined;
        default:
            throw new Error(`Unknown data type: ${data_type}`);
    }
}

