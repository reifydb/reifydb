/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class RowNumberValue implements Value {
    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("18446744073709551615");
    readonly type: Type = "RowNumber" as const;
    public readonly value?: bigint;

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;

            if (bigintValue < RowNumberValue.MIN_VALUE || bigintValue > RowNumberValue.MAX_VALUE) {
                throw new Error(`RowNumber value must be between ${RowNumberValue.MIN_VALUE} and ${RowNumberValue.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): RowNumberValue {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new RowNumberValue(undefined);
        }

        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as RowNumber`);
        }

        if (value < RowNumberValue.MIN_VALUE || value > RowNumberValue.MAX_VALUE) {
            throw new Error(`RowNumber value must be between ${RowNumberValue.MIN_VALUE} and ${RowNumberValue.MAX_VALUE}, got ${value}`);
        }

        return new RowNumberValue(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two RowNumber values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherRowNumber = other as RowNumberValue;
        return this.value === otherRowNumber.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}