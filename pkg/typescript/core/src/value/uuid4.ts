// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { v4 as uuidv4, NIL as NIL_UUID, validate, version } from 'uuid';
import { Type, Value, TypeValuePair } from ".";
import { NONE_VALUE } from "../constant";

/**
 * A UUID version 4 (random) value type
 */
export class Uuid4Value implements Value {
    readonly type: Type = "Uuid4" as const;
    private readonly uuid?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`Uuid4 value must be a string, got ${typeof value}`);
            }
            
            // Validate UUID format
            if (!validate(value)) {
                throw new Error(`Invalid UUID format: ${value}`);
            }
            
            // Check version (allow v4 or nil UUID)
            const ver = version(value);
            if (value !== NIL_UUID && ver !== 4) {
                throw new Error(`Invalid UUID version for Uuid4: expected v4, got v${ver}`);
            }
            
            this.uuid = value.toLowerCase();
        } else {
            this.uuid = undefined;
        }
    }

    /**
     * Generate a new random UUID v4
     */
    static generate(): Uuid4Value {
        return new Uuid4Value(uuidv4());
    }

    /**
     * Create a new Uuid4Value from a string
     */
    static new(uuid: string): Uuid4Value {
        return new Uuid4Value(uuid);
    }

    /**
     * Get the nil UUID (all zeros)
     */
    static nil(): Uuid4Value {
        return new Uuid4Value(NIL_UUID);
    }

    /**
     * Get default Uuid4Value (nil UUID)
     */
    static default(): Uuid4Value {
        return Uuid4Value.nil();
    }

    /**
     * Parse a UUID string
     */
    static parse(str: string): Uuid4Value {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === NONE_VALUE) {
            return new Uuid4Value(undefined);
        }

        // Try to parse as UUID
        if (!validate(trimmed)) {
            throw new Error(`Cannot parse "${str}" as Uuid4`);
        }

        const ver = version(trimmed);
        if (trimmed !== NIL_UUID && ver !== 4) {
            throw new Error(`Cannot parse "${str}" as Uuid4: wrong version (v${ver})`);
        }

        return new Uuid4Value(trimmed);
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
            return 'none';
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
     * Compare two UUID4 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUuid = other as Uuid4Value;
        return this.uuid === otherUuid.uuid;
    }

    /**
     * Compare two UUID4 values (for ordering)
     */
    compare(other: Uuid4Value): number {
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
            value: this.value === undefined ? NONE_VALUE : this.toString()
        };
    }
}