// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Int4Value implements Value {
    readonly type: Type = "Int4" as const;
    public readonly value: number;

    private static readonly MIN_VALUE = -2147483648;
    private static readonly MAX_VALUE = 2147483647;

    constructor(value: number) {
        if (value === undefined) {
            throw new Error(`Int4 value must be defined, a none is carried by NoneValue`);
        }
        if (!Number.isInteger(value)) {
            throw new Error(`Int4 value must be an integer, got ${value}`);
        }
        if (value < Int4Value.MIN_VALUE || value > Int4Value.MAX_VALUE) {
            throw new Error(`Int4 value must be between ${Int4Value.MIN_VALUE} and ${Int4Value.MAX_VALUE}, got ${value}`);
        }
        this.value = value;
    }

    static parse(str: string): Int4Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Int4`);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int4`);
        }
        
        return new Int4Value(num);
    }

    valueOf(): number {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
    }

    /**
     * Compare two Int4 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherInt = other as Int4Value;
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