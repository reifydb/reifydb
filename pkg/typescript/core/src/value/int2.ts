/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Int2Value implements Value {
    readonly type: Type = "Int2" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -32768;
    private static readonly MAX_VALUE = 32767;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int2 value must be an integer, got ${value}`);
            }
            if (value < Int2Value.MIN_VALUE || value > Int2Value.MAX_VALUE) {
                throw new Error(`Int2 value must be between ${Int2Value.MIN_VALUE} and ${Int2Value.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int2Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int2Value(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int2`);
        }
        
        return new Int2Value(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}