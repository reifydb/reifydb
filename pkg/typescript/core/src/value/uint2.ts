// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Uint2Value implements Value {
    readonly type: Type = "Uint2" as const;
    public readonly value: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 65535;

    constructor(value: number) {
        if (value === undefined) {
            throw new Error(`Uint2 value must be defined, a none is carried by NoneValue`);
        }
        if (!Number.isInteger(value)) {
            throw new Error(`Uint2 value must be an integer, got ${value}`);
        }
        if (value < Uint2Value.MIN_VALUE || value > Uint2Value.MAX_VALUE) {
            throw new Error(`Uint2 value must be between ${Uint2Value.MIN_VALUE} and ${Uint2Value.MAX_VALUE}, got ${value}`);
        }
        this.value = value;
    }

    static parse(str: string): Uint2Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Uint2`);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint2`);
        }
        
        return new Uint2Value(num);
    }

    valueOf(): number {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
    }

    /**
     * Compare two Uint2 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUint = other as Uint2Value;
        return this.value === otherUint.value;
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