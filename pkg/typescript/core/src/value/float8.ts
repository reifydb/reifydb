// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Float8Value implements Value {
    readonly type: Type = "Float8" as const;
    public readonly value: number;

    constructor(value: number) {
        if (value === undefined) {
            throw new Error(`Float8 value must be defined, a none is carried by NoneValue`);
        }
        if (typeof value !== 'number') {
            throw new Error(`Float8 value must be a number, got ${typeof value}`);
        }
        if (Number.isNaN(value) || !Number.isFinite(value)) {
            throw new Error(`Float8 value must be finite, got ${value}`);
        }
        this.value = value;
    }

    static parse(str: string): Float8Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Float8`);
        }

        const num = Number(trimmed);

        if (Number.isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Float8`);
        }

        return new Float8Value(num);
    }

    valueOf(): number {
        return this.value;
    }

    toString(): string {
        return this.value.toString();
    }

    /**
     * Compare two Float8 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }

        const otherFloat = other as Float8Value;
        if (this.value === undefined || otherFloat.value === undefined) {
            return this.value === otherFloat.value;
        }
        const epsilon = 1e-14;
        return Math.abs(this.value - otherFloat.value) <= epsilon;
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
