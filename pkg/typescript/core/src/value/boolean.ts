// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class BooleanValue implements Value {
    readonly type: Type = "Boolean" as const;
    public readonly value?: boolean;

    constructor(value?: boolean) {
        if (value !== undefined) {
            if (typeof value !== 'boolean') {
                throw new Error(`Boolean value must be a boolean, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): BooleanValue {
        const trimmed = str.trim().toLowerCase();

        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new BooleanValue(undefined);
        }

        if (trimmed === 'true') {
            return new BooleanValue(true);
        }

        if (trimmed === 'false') {
            return new BooleanValue(false);
        }

        throw new Error(`Cannot parse "${str}" as Boolean`);
    }

    valueOf(): boolean | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    /**
     * Compare two boolean values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherBoolean = other as BooleanValue;
        return this.value === otherBoolean.value;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}