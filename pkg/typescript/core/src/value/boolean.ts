// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class BooleanValue implements Value {
    readonly type: Type = "Boolean" as const;
    public readonly value: boolean;

    constructor(value: boolean) {
        if (value === undefined) {
            throw new Error(`Boolean value must be defined, a none is carried by NoneValue`);
        }
        if (typeof value !== 'boolean') {
            throw new Error(`Boolean value must be a boolean, got ${typeof value}`);
        }
        this.value = value;
    }

    static parse(str: string): BooleanValue {
        const trimmed = str.trim().toLowerCase();

        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Boolean`);
        }

        if (trimmed === 'true') {
            return new BooleanValue(true);
        }

        if (trimmed === 'false') {
            return new BooleanValue(false);
        }

        throw new Error(`Cannot parse "${str}" as Boolean`);
    }

    valueOf(): boolean {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
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

    toJSON(): boolean {
        return this.value ?? null;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.toString()
        };
    }
}