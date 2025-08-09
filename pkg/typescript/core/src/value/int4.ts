/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Int4Value implements Value {
    readonly type: Type = "Int4" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -2147483648;
    private static readonly MAX_VALUE = 2147483647;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int4 value must be an integer, got ${value}`);
            }
            if (value < Int4Value.MIN_VALUE || value > Int4Value.MAX_VALUE) {
                throw new Error(`Int4 value must be between ${Int4Value.MIN_VALUE} and ${Int4Value.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int4Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int4Value(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int4`);
        }
        
        return new Int4Value(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}