// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class DecimalValue implements Value {
    readonly type: Type = "Decimal" as const;
    public readonly value?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`Decimal value must be a string, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): DecimalValue {
        if (str === UNDEFINED_VALUE) {
            return new DecimalValue(undefined);
        }
        
        return new DecimalValue(str);
    }

    valueOf(): string | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value;
    }

    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherDecimal = other as DecimalValue;
        return this.value === otherDecimal.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}
