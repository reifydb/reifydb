// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class Utf8Value implements Value {
    readonly type: Type = "Utf8" as const;
    public readonly value: string;

    constructor(value: string) {
        if (value === undefined) {
            throw new Error(`Utf8 value must be defined, a none is carried by NoneValue`);
        }
        if (typeof value !== 'string') {
            throw new Error(`Utf8 value must be a string, got ${typeof value}`);
        }
        this.value = value;
    }

    static parse(str: string): Utf8Value {
        if (str === NONE_VALUE) {
            throw new Error(`Cannot parse "${str}" as Utf8`);
        }
        
        return new Utf8Value(str);
    }

    valueOf(): string {
        return this.value;
    }

    toString(): string {
        return this.value;
    }

    /**
     * Compare two Utf8 values for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherUtf8 = other as Utf8Value;
        return this.value === otherUtf8.value;
    }

    toJSON(): string {
        return this.value ?? null;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.toString()
        };
    }
}