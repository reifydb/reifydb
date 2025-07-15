/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {DataType} from "./types";

const UNDEFINED_VALUE = "⟪undefined⟫";

/**
 * Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S) to nanoseconds
 */
function parseInterval(value: string): bigint {
    console.log(value);
    if (!value.startsWith('P')) {
        throw new Error('Invalid interval format - must start with P');
    }
    
    let totalNanos = 0n;
    let currentNumber = '';
    let inTimePart = false;
    
    for (let i = 1; i < value.length; i++) {
        const char = value[i];
        
        if (char === 'T') {
            inTimePart = true;
            continue;
        }
        
        if (char >= '0' && char <= '9') {
            currentNumber += char;
            continue;
        }
        
        const num = BigInt(currentNumber);
        currentNumber = '';
        
        switch (char) {
            case 'Y':
                if (inTimePart) throw new Error('Years not allowed in time part');
                totalNanos += num * 365n * 24n * 60n * 60n * 1_000_000_000n; // Approximate
                break;
            case 'M':
                if (inTimePart) {
                    totalNanos += num * 60n * 1_000_000_000n; // Minutes
                } else {
                    totalNanos += num * 30n * 24n * 60n * 60n * 1_000_000_000n; // Months (approximate)
                }
                break;
            case 'W':
                if (inTimePart) throw new Error('Weeks not allowed in time part');
                totalNanos += num * 7n * 24n * 60n * 60n * 1_000_000_000n;
                break;
            case 'D':
                if (inTimePart) throw new Error('Days not allowed in time part');
                totalNanos += num * 24n * 60n * 60n * 1_000_000_000n;
                break;
            case 'H':
                if (!inTimePart) throw new Error('Hours only allowed in time part');
                totalNanos += num * 60n * 60n * 1_000_000_000n;
                break;
            case 'S':
                if (!inTimePart) throw new Error('Seconds only allowed in time part');
                totalNanos += num * 1_000_000_000n;
                break;
            default:
                throw new Error(`Invalid character in interval: ${char}`);
        }
    }
    
    if (currentNumber) {
        throw new Error('Incomplete interval specification');
    }
    
    return totalNanos;
}

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
            // Return interval as duration in nanoseconds
            return parseInterval(value);
        case "Undefined":
            return undefined;
        default:
            throw new Error(`Unknown data type: ${data_type}`);
    }
}

