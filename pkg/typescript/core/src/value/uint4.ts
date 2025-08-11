/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Uint4Value implements Value {
    readonly type: Type = "Uint4" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 4294967295;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Uint4 value must be an integer, got ${value}`);
            }
            if (value < Uint4Value.MIN_VALUE || value > Uint4Value.MAX_VALUE) {
                throw new Error(`Uint4 value must be between ${Uint4Value.MIN_VALUE} and ${Uint4Value.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Uint4Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint4Value(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint4`);
        }
        
        return new Uint4Value(num);
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