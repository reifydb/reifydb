/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Utf8Value implements Value {
    readonly type: Type = "Utf8" as const;
    public readonly value?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`Utf8 value must be a string, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Utf8Value {
        if (str === UNDEFINED_VALUE) {
            return new Utf8Value(undefined);
        }
        
        return new Utf8Value(str);
    }

    valueOf(): string | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value;
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

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}