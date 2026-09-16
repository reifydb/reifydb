// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

const DECIMAL_PATTERN = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;

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
        if (!DECIMAL_PATTERN.test(str)) {
            throw new Error(`Cannot parse "${str}" as Decimal`);
        }

        return new DecimalValue(str);
    }

    valueOf(): string | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'none' : this.value;
    }

    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherDecimal = other as DecimalValue;
        return this.value === otherDecimal.value;
    }

    toJSON(): string | null {
        return this.value ?? null;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? NONE_VALUE : this.toString()
        };
    }
}
