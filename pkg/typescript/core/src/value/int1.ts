/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Int1Value implements Value {
    readonly type: Type = "Int1" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -128;
    private static readonly MAX_VALUE = 127;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int1 value must be an integer, got ${value}`);
            }
            if (value < Int1Value.MIN_VALUE || value > Int1Value.MAX_VALUE) {
                throw new Error(`Int1 value must be between ${Int1Value.MIN_VALUE} and ${Int1Value.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int1Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int1Value(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int1`);
        }
        
        return new Int1Value(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two Int1 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherInt = other as Int1Value;
        return this.value === otherInt.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}