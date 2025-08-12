/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Uint8Value implements Value {
    readonly type: Type = "Uint8" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("18446744073709551615");

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;
            
            if (bigintValue < Uint8Value.MIN_VALUE || bigintValue > Uint8Value.MAX_VALUE) {
                throw new Error(`Uint8 value must be between ${Uint8Value.MIN_VALUE} and ${Uint8Value.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Uint8Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint8Value(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Uint8`);
        }
        
        if (value < Uint8Value.MIN_VALUE || value > Uint8Value.MAX_VALUE) {
            throw new Error(`Uint8 value must be between ${Uint8Value.MIN_VALUE} and ${Uint8Value.MAX_VALUE}, got ${value}`);
        }
        
        return new Uint8Value(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toNumber(): number | undefined {
        if (this.value === undefined) return undefined;
        return Number(this.value);
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two Uint8 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUint = other as Uint8Value;
        return this.value === otherUint.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}