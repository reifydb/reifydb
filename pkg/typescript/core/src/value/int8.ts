// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Int8Value implements Value {
    readonly type: Type = "Int8" as const;
    public readonly value: bigint;

    private static readonly MIN_VALUE = BigInt("-9223372036854775808");
    private static readonly MAX_VALUE = BigInt("9223372036854775807");

    constructor(value: bigint | number) {
        if (value === undefined) {
            throw new Error(`Int8 value must be defined, a none is carried by NoneValue`);
        }
        const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;
        
        if (bigintValue < Int8Value.MIN_VALUE || bigintValue > Int8Value.MAX_VALUE) {
            throw new Error(`Int8 value must be between ${Int8Value.MIN_VALUE} and ${Int8Value.MAX_VALUE}, got ${bigintValue}`);
        }
        this.value = bigintValue;
    }

    static parse(str: string): Int8Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Int8`);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Int8`);
        }
        
        if (value < Int8Value.MIN_VALUE || value > Int8Value.MAX_VALUE) {
            throw new Error(`Int8 value must be between ${Int8Value.MIN_VALUE} and ${Int8Value.MAX_VALUE}, got ${value}`);
        }
        
        return new Int8Value(value);
    }

    valueOf(): bigint {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
    }

    /**
     * Compare two Int8 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherInt = other as Int8Value;
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