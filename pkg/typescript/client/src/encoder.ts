/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Type } from "@reifydb/core";

export interface TypeValuePair {
    type: Type;
    value: string;
}

/**
 * Encodes a JavaScript value into a TypeValuePair for ReifyDB
 * @param value - The value to encode
 * @returns A TypeValuePair with the appropriate type and string representation
 */
export function encodeValue(value: any): TypeValuePair {
    // Handle null and undefined
    if (value === null || value === undefined) {
        return { type: 'Undefined', value: '⟪undefined⟫' };
    }
    
    // Handle Value objects that have an encode method
    if (value && typeof value === 'object' && 'encode' in value && typeof value.encode === 'function') {
        return value.encode();
    }
    
    // Handle booleans
    if (typeof value === 'boolean') {
        return { type: 'Bool', value: value.toString() };
    }
    
    // Handle numbers
    if (typeof value === 'number') {
        if (Number.isInteger(value)) {
            // Choose appropriate integer type based on value range
            if (value >= -128 && value <= 127) {
                return { type: 'Int1', value: value.toString() };
            } else if (value >= -32768 && value <= 32767) {
                return { type: 'Int2', value: value.toString() };
            } else if (value >= -2147483648 && value <= 2147483647) {
                return { type: 'Int4', value: value.toString() };
            } else {
                return { type: 'Int8', value: value.toString() };
            }
        } else {
            // Floating point number
            return { type: 'Float8', value: value.toString() };
        }
    }
    
    // Handle strings
    if (typeof value === 'string') {
        // Check if it looks like a UUID
        if (/^[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
            // Determine UUID version based on version field
            const version = value[14];
            if (version === '4') {
                return { type: 'Uuid4', value: value };
            } else if (version === '7') {
                return { type: 'Uuid7', value: value };
            }
        }
        // Default to UTF-8 string
        return { type: 'Utf8', value: value };
    }
    
    // Handle bigint
    if (typeof value === 'bigint') {
        // Choose appropriate type based on value range
        if (value >= 0n) {
            if (value <= 255n) {
                return { type: 'Uint1', value: value.toString() };
            } else if (value <= 65535n) {
                return { type: 'Uint2', value: value.toString() };
            } else if (value <= 4294967295n) {
                return { type: 'Uint4', value: value.toString() };
            } else if (value <= 18446744073709551615n) {
                return { type: 'Uint8', value: value.toString() };
            } else {
                return { type: 'Uint16', value: value.toString() };
            }
        } else {
            // Negative bigint - use signed types
            if (value >= -9223372036854775808n) {
                return { type: 'Int8', value: value.toString() };
            } else {
                return { type: 'Int16', value: value.toString() };
            }
        }
    }
    
    // Handle Date objects
    if (value instanceof Date) {
        return { type: 'DateTime', value: value.toISOString() };
    }
    
    // Handle Uint8Array (Blob)
    if (value instanceof Uint8Array) {
        // Convert to hex string
        const hex = Array.from(value)
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
        return { type: 'Blob', value: '0x' + hex };
    }
    
    // Handle ArrayBuffer
    if (value instanceof ArrayBuffer) {
        const uint8Array = new Uint8Array(value);
        const hex = Array.from(uint8Array)
            .map(b => b.toString(16).padStart(2, '0'))
            .join('');
        return { type: 'Blob', value: '0x' + hex };
    }
    
    // Fallback - try to convert to string
    throw new Error(`Cannot encode value of type ${typeof value}: ${value}`);
}

/**
 * Encodes parameters for ReifyDB commands/queries
 * @param params - Either an array of positional parameters or an object of named parameters
 * @returns Encoded parameters ready for transmission
 */
export function encodeParams(params: any): TypeValuePair[] | Record<string, TypeValuePair> {
    if (params === undefined || params === null) {
        return [];
    }
    
    if (Array.isArray(params)) {
        // Positional parameters
        return params.map(param => encodeValue(param));
    } else if (typeof params === 'object') {
        // Named parameters
        const encoded: Record<string, TypeValuePair> = {};
        for (const [key, value] of Object.entries(params)) {
            encoded[key] = encodeValue(value);
        }
        return encoded;
    }
    
    throw new Error(`Invalid parameters type: expected array or object, got ${typeof params}`);
}