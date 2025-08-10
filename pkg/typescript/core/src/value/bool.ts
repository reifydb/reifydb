/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class BoolValue implements Value {
    readonly type: Type = "Bool" as const;
    public readonly value?: boolean;

    constructor(value?: boolean) {
        if (value !== undefined) {
            if (typeof value !== 'boolean') {
                throw new Error(`Bool value must be a boolean, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): BoolValue {
        const trimmed = str.trim().toLowerCase();

        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new BoolValue(undefined);
        }

        if (trimmed === 'true') {
            return new BoolValue(true);
        }

        if (trimmed === 'false') {
            return new BoolValue(false);
        }

        throw new Error(`Cannot parse "${str}" as Bool`);
    }

    valueOf(): boolean | undefined {
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