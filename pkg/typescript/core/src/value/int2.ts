// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Int2Value implements Value {
    readonly type: Type = "Int2" as const;
    public readonly value: number;

    private static readonly MIN_VALUE = -32768;
    private static readonly MAX_VALUE = 32767;

    constructor(value: number) {
        if (value === undefined) {
            throw new Error(`Int2 value must be defined, a none is carried by NoneValue`);
        }
        if (!Number.isInteger(value)) {
            throw new Error(`Int2 value must be an integer, got ${value}`);
        }
        if (value < Int2Value.MIN_VALUE || value > Int2Value.MAX_VALUE) {
            throw new Error(`Int2 value must be between ${Int2Value.MIN_VALUE} and ${Int2Value.MAX_VALUE}, got ${value}`);
        }
        this.value = value;
    }

    static parse(str: string): Int2Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Int2`);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int2`);
        }
        
        return new Int2Value(num);
    }

    valueOf(): number {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
    }

    /**
     * Compare two Int2 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherInt = other as Int2Value;
        return this.value === otherInt.value;
    }

    toJSON(): string {
        return this.value.toString();
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.toString()
        };
    }
}