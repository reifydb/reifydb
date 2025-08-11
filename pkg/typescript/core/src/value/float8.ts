/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Float8Value implements Value {
    readonly type: Type = "Float8" as const;
    public readonly value?: number;

    constructor(value?: number) {
        if (value !== undefined) {
            if (typeof value !== 'number') {
                throw new Error(`Float8 value must be a number, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Float8Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Float8Value(undefined);
        }

        const num = Number(trimmed);

        if (Number.isNaN(num) && trimmed.toLowerCase() !== 'nan') {
            throw new Error(`Cannot parse "${str}" as Float8`);
        }

        return new Float8Value(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }

    toString(): string {
        return this.value === undefined ? 'undefined' : this.value.toString();
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}
