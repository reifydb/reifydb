// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { v7 as uuidv7, NIL as NIL_UUID, validate, version } from 'uuid';
import { Type, Value, TypeValuePair } from ".";
import { UNDEFINED_VALUE } from "../constant";

/**
 * A UUID version 7 (timestamp-based) value type
 */
export class Uuid7Value implements Value {
    readonly type: Type = "Uuid7" as const;
    private readonly uuid?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`Uuid7 value must be a string, got ${typeof value}`);
            }
            
            // Validate UUID format
            if (!validate(value)) {
                throw new Error(`Invalid UUID format: ${value}`);
            }
            
            // Check version (allow v7 or nil UUID)
            const ver = version(value);
            if (value !== NIL_UUID && ver !== 7) {
                throw new Error(`Invalid UUID version for Uuid7: expected v7, got v${ver}`);
            }
            
            this.uuid = value.toLowerCase();
        } else {
            this.uuid = undefined;
        }
    }

    /**
     * Generate a new timestamp-based UUID v7
     */
    static generate(): Uuid7Value {
        return new Uuid7Value(uuidv7());
    }

    /**
     * Create a new Uuid7Value from a string
     */
    static new(uuid: string): Uuid7Value {
        return new Uuid7Value(uuid);
    }

    /**
     * Get the nil UUID (all zeros)
     */
    static nil(): Uuid7Value {
        return new Uuid7Value(NIL_UUID);
    }

    /**
     * Get default Uuid7Value (nil UUID)
     */
    static default(): Uuid7Value {
        return Uuid7Value.nil();
    }

    /**
     * Parse a UUID string
     */
    static parse(str: string): Uuid7Value {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uuid7Value(undefined);
        }

        // Try to parse as UUID
        if (!validate(trimmed)) {
            throw new Error(`Cannot parse "${str}" as Uuid7`);
        }

        const ver = version(trimmed);
        if (trimmed !== NIL_UUID && ver !== 7) {
            throw new Error(`Cannot parse "${str}" as Uuid7: wrong version (v${ver})`);
        }

        return new Uuid7Value(trimmed);
    }

    /**
     * Get the UUID string
     */
    asString(): string | undefined {
        return this.uuid;
    }

    /**
     * Get the UUID as bytes (16-byte array)
     */
    asBytes(): Uint8Array | undefined {
        if (this.uuid === undefined) return undefined;
        
        // Remove hyphens and convert hex to bytes
        const hex = this.uuid.replace(/-/g, '');
        const bytes = new Uint8Array(16);
        for (let i = 0; i < 16; i++) {
            bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    /**
     * Extract the timestamp from UUID v7 (milliseconds since Unix epoch)
     */
    getTimestamp(): number | undefined {
        if (this.uuid === undefined || this.uuid === NIL_UUID) return undefined;
        
        // UUID v7 has a 48-bit timestamp in the first 6 bytes
        const hex = this.uuid.replace(/-/g, '');
        const timestampHex = hex.substring(0, 12);
        const timestamp = parseInt(timestampHex, 16);
        return timestamp;
    }

    /**
     * Check if this is the nil UUID
     */
    isNil(): boolean {
        return this.uuid === NIL_UUID;
    }

    /**
     * Get the UUID version
     */
    getVersion(): number | undefined {
        if (this.uuid === undefined) return undefined;
        return version(this.uuid);
    }

    /**
     * Format as string
     */
    toString(): string {
        if (this.uuid === undefined) {
            return 'undefined';
        }
        return this.uuid;
    }

    valueOf(): string | undefined {
        return this.uuid;
    }

    /**
     * Get the internal representation
     */
    get value(): string | undefined {
        return this.valueOf();
    }

    /**
     * Compare two UUID7 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUuid = other as Uuid7Value;
        return this.uuid === otherUuid.uuid;
    }

    /**
     * Compare two UUID7 values (for ordering)
     * UUID v7 has timestamp-based ordering for UUIDs generated close in time
     */
    compare(other: Uuid7Value): number {
        if (this.uuid === undefined || other.uuid === undefined) {
            if (this.uuid === other.uuid) return 0;
            if (this.uuid === undefined) return -1;
            return 1;
        }
        
        // Compare as bytes for consistent ordering
        const thisBytes = this.asBytes()!;
        const otherBytes = other.asBytes()!;
        
        for (let i = 0; i < 16; i++) {
            if (thisBytes[i] < otherBytes[i]) return -1;
            if (thisBytes[i] > otherBytes[i]) return 1;
        }
        
        return 0;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}