/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from ".";
import {UNDEFINED_VALUE} from "../constant";

export class Uint1Value implements Value {
    readonly type: Type = "Uint1" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 255;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Uint1 value must be an integer, got ${value}`);
            }
            if (value < Uint1Value.MIN_VALUE || value > Uint1Value.MAX_VALUE) {
                throw new Error(`Uint1 value must be between ${Uint1Value.MIN_VALUE} and ${Uint1Value.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Uint1Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint1Value(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint1`);
        }
        
        return new Uint1Value(num);
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