/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class RowIdValue implements Value {
    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("18446744073709551615");
    readonly type: Type = "RowId" as const;
    public readonly value?: bigint;

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;

            if (bigintValue < RowIdValue.MIN_VALUE || bigintValue > RowIdValue.MAX_VALUE) {
                throw new Error(`RowId value must be between ${RowIdValue.MIN_VALUE} and ${RowIdValue.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): RowIdValue {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new RowIdValue(undefined);
        }

        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as RowId`);
        }

        if (value < RowIdValue.MIN_VALUE || value > RowIdValue.MAX_VALUE) {
            throw new Error(`RowId value must be between ${RowIdValue.MIN_VALUE} and ${RowIdValue.MAX_VALUE}, got ${value}`);
        }

        return new RowIdValue(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two RowId values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherRowId = other as RowIdValue;
        return this.value === otherRowId.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}