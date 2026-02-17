// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

/**
 * A binary large object (BLOB) value that can hold arbitrary byte data.
 */
export class BlobValue implements Value {
    readonly type: Type = "Blob" as const;
    private readonly bytes?: Uint8Array;

    constructor(value?: Uint8Array | ArrayBuffer | string | number[]) {
        if (value !== undefined) {
            if (value instanceof Uint8Array) {
                this.bytes = new Uint8Array(value);
            } else if (value instanceof ArrayBuffer) {
                this.bytes = new Uint8Array(value);
            } else if (typeof value === 'string') {
                // Try to parse as hex or base64
                const parsed = BlobValue.parseString(value);
                if (parsed === null) {
                    throw new Error(`Invalid blob string: ${value}`);
                }
                this.bytes = parsed;
            } else if (Array.isArray(value)) {
                // Array of numbers (bytes)
                for (const byte of value) {
                    if (!Number.isInteger(byte) || byte < 0 || byte > 255) {
                        throw new Error(`Invalid byte value: ${byte}`);
                    }
                }
                this.bytes = new Uint8Array(value);
            } else {
                throw new Error(`Blob value must be a Uint8Array, ArrayBuffer, string, or number[], got ${typeof value}`);
            }
        } else {
            this.bytes = undefined;
        }
    }

    /**
     * Create a new BLOB from raw bytes
     */
    static new(bytes: Uint8Array | number[]): BlobValue {
        return new BlobValue(bytes);
    }

    /**
     * Create an empty BLOB
     */
    static empty(): BlobValue {
        return new BlobValue(new Uint8Array(0));
    }

    /**
     * Create a BLOB from a byte array
     */
    static fromBytes(bytes: number[]): BlobValue {
        return new BlobValue(bytes);
    }

    /**
     * Create a BLOB from a hex string
     */
    static fromHex(hex: string): BlobValue {
        // Remove 0x prefix if present
        const cleanHex = hex.startsWith('0x') || hex.startsWith('0X')
            ? hex.substring(2)
            : hex;

        if (cleanHex.length % 2 !== 0) {
            throw new Error(`Invalid hex string: odd length`);
        }

        const bytes = new Uint8Array(cleanHex.length / 2);
        for (let i = 0; i < cleanHex.length; i += 2) {
            const byte = parseInt(cleanHex.substring(i, i + 2), 16);
            if (isNaN(byte)) {
                throw new Error(`Invalid hex string: ${hex}`);
            }
            bytes[i / 2] = byte;
        }
        return new BlobValue(bytes);
    }

    /**
     * Create a BLOB from a base64 string
     */
    static fromBase64(base64: string): BlobValue {
        // Validate base64 format
        if (!/^[A-Za-z0-9+/]*={0,2}$/.test(base64)) {
            throw new Error(`Invalid base64 string: ${base64}`);
        }

        try {
            if (typeof Buffer !== 'undefined') {
                // Node.js environment
                const buffer = Buffer.from(base64, 'base64');
                return new BlobValue(new Uint8Array(buffer));
            } else {
                // Browser environment
                const binaryString = atob(base64);
                const bytes = new Uint8Array(binaryString.length);
                for (let i = 0; i < binaryString.length; i++) {
                    bytes[i] = binaryString.charCodeAt(i);
                }
                return new BlobValue(bytes);
            }
        } catch (e) {
            throw new Error(`Invalid base64 string: ${base64}`);
        }
    }

    /**
     * Create a BLOB from a UTF-8 string
     */
    static fromUtf8(str: string): BlobValue {
        const encoder = new TextEncoder();
        return new BlobValue(encoder.encode(str));
    }

    /**
     * Parse a blob string (hex or undefined)
     */
    static parse(str: string): BlobValue {
        const trimmed = str.trim();

        if (trimmed === '' || trimmed === NONE_VALUE) {
            return new BlobValue(undefined);
        }

        const parsed = BlobValue.parseString(trimmed);
        if (parsed === null) {
            throw new Error(`Cannot parse "${str}" as Blob`);
        }

        return new BlobValue(parsed);
    }

    /**
     * Get the raw bytes
     */
    asBytes(): Uint8Array | undefined {
        return this.bytes ? new Uint8Array(this.bytes) : undefined;
    }

    /**
     * Get the length in bytes
     */
    length(): number {
        return this.bytes?.length ?? 0;
    }

    /**
     * Check if the BLOB is empty
     */
    isEmpty(): boolean {
        return this.bytes?.length === 0;
    }

    /**
     * Convert to hex string with 0x prefix
     */
    toHex(): string | undefined {
        if (this.bytes === undefined) return undefined;

        const hex = Array.from(this.bytes)
            .map(byte => byte.toString(16).padStart(2, '0'))
            .join('');
        return '0x' + hex;
    }

    /**
     * Convert to base64 string
     */
    toBase64(): string | undefined {
        if (this.bytes === undefined) return undefined;

        if (typeof Buffer !== 'undefined') {
            // Node.js environment
            return Buffer.from(this.bytes).toString('base64');
        } else {
            // Browser environment
            const binaryString = Array.from(this.bytes)
                .map(byte => String.fromCharCode(byte))
                .join('');
            return btoa(binaryString);
        }
    }

    /**
     * Convert to UTF-8 string
     */
    toUtf8(): string | undefined {
        if (this.bytes === undefined) return undefined;

        const decoder = new TextDecoder();
        return decoder.decode(this.bytes);
    }

    /**
     * Format as hex string with 0x prefix
     */
    toString(): string {
        if (this.bytes === undefined) {
            return 'none';
        }
        return this.toHex()!;
    }

    valueOf(): Uint8Array | undefined {
        return this.bytes ? new Uint8Array(this.bytes) : undefined;
    }

    /**
     * Get the internal representation
     */
    get value(): Uint8Array | undefined {
        return this.valueOf();
    }

    /**
     * Compare two blobs for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherBlob = other as BlobValue;
        if (this.bytes === undefined || otherBlob.bytes === undefined) {
            return this.bytes === otherBlob.bytes;
        }

        if (this.bytes.length !== otherBlob.bytes.length) {
            return false;
        }

        for (let i = 0; i < this.bytes.length; i++) {
            if (this.bytes[i] !== otherBlob.bytes[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Helper to parse a string as hex
     */
    private static parseString(str: string): Uint8Array | null {
        // Try hex first (with or without 0x prefix)
        if (str.startsWith('0x') || str.startsWith('0X')) {
            try {
                return BlobValue.fromHex(str).bytes!;
            } catch {
                return null;
            }
        }

        // Check if it looks like hex (all hex digits)
        if (/^[0-9a-fA-F]+$/.test(str) && str.length % 2 === 0) {
            try {
                return BlobValue.fromHex(str).bytes!;
            } catch {
                return null;
            }
        }

        return null;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? NONE_VALUE : this.toString()
        };
    }
}