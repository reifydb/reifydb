// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Uint16Value implements Value {
    readonly type: Type = "Uint16" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("340282366920938463463374607431768211455");

    constructor(value?: bigint | number | string) {
        if (value !== undefined) {
            let bigintValue: bigint;
            
            if (typeof value === 'string') {
                try {
                    bigintValue = BigInt(value);
                } catch (e) {
                    throw new Error(`Uint16 value must be a valid integer, got ${value}`);
                }
            } else if (typeof value === 'number') {
                bigintValue = BigInt(Math.trunc(value));
            } else {
                bigintValue = value;
            }
            
            if (bigintValue < Uint16Value.MIN_VALUE || bigintValue > Uint16Value.MAX_VALUE) {
                throw new Error(`Uint16 value must be between ${Uint16Value.MIN_VALUE} and ${Uint16Value.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Uint16Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint16Value(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Uint16`);
        }
        
        if (value < Uint16Value.MIN_VALUE || value > Uint16Value.MAX_VALUE) {
            throw new Error(`Uint16 value must be between ${Uint16Value.MIN_VALUE} and ${Uint16Value.MAX_VALUE}, got ${value}`);
        }
        
        return new Uint16Value(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two Uint16 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUint = other as Uint16Value;
        return this.value === otherUint.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}