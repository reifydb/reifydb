// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { v7 as uuidv7, NIL as NIL_UUID, validate, version } from 'uuid';
import { Type, Value, TypeValuePair } from ".";
import { UNDEFINED_VALUE } from "../constant";

/**
 * An IdentityId value type that wraps a UUID v7
 */
export class IdentityIdValue implements Value {
    readonly type: Type = "IdentityId" as const;
    public readonly value?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`IdentityId value must be a string, got ${typeof value}`);
            }
            
            // Validate UUID format
            if (!validate(value)) {
                throw new Error(`Invalid UUID format for IdentityId: ${value}`);
            }
            
            // Check version (allow v7 or nil UUID)
            const ver = version(value);
            if (value !== NIL_UUID && ver !== 7) {
                throw new Error(`Invalid UUID version for IdentityId: expected v7, got v${ver}`);
            }
            
            this.value = value.toLowerCase();
        } else {
            this.value = undefined;
        }
    }

    /**
     * Generate a new IdentityId with a UUID v7
     */
    static generate(): IdentityIdValue {
        return new IdentityIdValue(uuidv7());
    }

    /**
     * Get the nil IdentityId (all zeros)
     */
    static nil(): IdentityIdValue {
        return new IdentityIdValue(NIL_UUID);
    }

    /**
     * Parse a string as an IdentityId
     */
    static parse(str: string): IdentityIdValue {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new IdentityIdValue(undefined);
        }

        // Try to parse as UUID
        if (!validate(trimmed)) {
            throw new Error(`Cannot parse "${str}" as IdentityId`);
        }

        const ver = version(trimmed);
        if (trimmed !== NIL_UUID && ver !== 7) {
            throw new Error(`Cannot parse "${str}" as IdentityId: wrong UUID version (v${ver})`);
        }

        return new IdentityIdValue(trimmed);
    }

    /**
     * Get the UUID string value
     */
    valueOf(): string | undefined {
        return this.value;
    }

    /**
     * Format as string
     */
    toString(): string {
        return this.value === undefined ? 'undefined' : this.value;
    }

    /**
     * Extract the timestamp from the UUID v7 (milliseconds since Unix epoch)
     */
    getTimestamp(): number | undefined {
        if (this.value === undefined || this.value === NIL_UUID) return undefined;
        
        // UUID v7 has a 48-bit timestamp in the first 6 bytes
        const hex = this.value.replace(/-/g, '');
        const timestampHex = hex.substring(0, 12);
        const timestamp = parseInt(timestampHex, 16);
        return timestamp;
    }

    /**
     * Check if this is the nil UUID
     */
    isNil(): boolean {
        return this.value === NIL_UUID;
    }

    /**
     * Compare two IdentityId values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherIdentityId = other as IdentityIdValue;
        return this.value === otherIdentityId.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}