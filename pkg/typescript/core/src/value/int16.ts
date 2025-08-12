/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Int16Value implements Value {
    readonly type: Type = "Int16" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt("-170141183460469231731687303715884105728");
    private static readonly MAX_VALUE = BigInt("170141183460469231731687303715884105727");

    constructor(value?: bigint | number | string) {
        if (value !== undefined) {
            let bigintValue: bigint;
            
            if (typeof value === 'string') {
                try {
                    bigintValue = BigInt(value);
                } catch (e) {
                    throw new Error(`Int16 value must be a valid integer, got ${value}`);
                }
            } else if (typeof value === 'number') {
                bigintValue = BigInt(Math.trunc(value));
            } else {
                bigintValue = value;
            }
            
            if (bigintValue < Int16Value.MIN_VALUE || bigintValue > Int16Value.MAX_VALUE) {
                throw new Error(`Int16 value must be between ${Int16Value.MIN_VALUE} and ${Int16Value.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Int16Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int16Value(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Int16`);
        }
        
        if (value < Int16Value.MIN_VALUE || value > Int16Value.MAX_VALUE) {
            throw new Error(`Int16 value must be between ${Int16Value.MIN_VALUE} and ${Int16Value.MAX_VALUE}, got ${value}`);
        }
        
        return new Int16Value(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two Int16 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherInt = other as Int16Value;
        return this.value === otherInt.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}