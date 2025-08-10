/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from "./type";
import {UNDEFINED_VALUE} from "../constant";

/**
 * An undefined value wrapper type
 */
export class UndefinedValue implements Value {
    readonly type: Type = "Undefined" as const;

    constructor() {
        // Always undefined, no parameters needed
    }

    /**
     * Create a new UndefinedValue
     */
    static new(): UndefinedValue {
        return new UndefinedValue();
    }

    /**
     * Get default UndefinedValue
     */
    static default(): UndefinedValue {
        return new UndefinedValue();
    }

    /**
     * Parse a string as undefined
     */
    static parse(str: string): UndefinedValue {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE || trimmed === 'undefined') {
            return new UndefinedValue();
        }
        throw new Error(`Cannot parse "${str}" as Undefined`);
    }

    /**
     * Check if this value is undefined (always true)
     */
    isUndefined(): boolean {
        return true;
    }

    /**
     * Format as string
     */
    toString(): string {
        return 'undefined';
    }

    valueOf(): undefined {
        return undefined;
    }

    /**
     * Get the internal representation (always undefined)
     */
    get value(): undefined {
        return undefined;
    }

    /**
     * Compare two undefined values (always equal)
     */
    equals(other: UndefinedValue): boolean {
        return true;
    }

    /**
     * Compare two undefined values for ordering (always equal)
     */
    compare(other: UndefinedValue): number {
        return 0;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}