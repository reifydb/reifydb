/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Float4Value implements Value {

    private static readonly MAX_VALUE = 3.4028235e+38;
    private static readonly MIN_VALUE = -3.4028235e+38;
    private static readonly MIN_POSITIVE = 1.175494e-38;

    readonly type: Type = "Float4" as const;
    public readonly value?: number;

    constructor(value?: number) {
        if (value !== undefined) {
            if (typeof value !== 'number') {
                throw new Error(`Float4 value must be a number, got ${typeof value}`);
            }

            if (Number.isNaN(value) || !Number.isFinite(value)) {
                this.value = undefined;
            } else {
                // Clamp to Float32 range
                if (value !== 0 && Math.abs(value) < Float4Value.MIN_POSITIVE) {
                    // Underflow to zero
                    this.value = 0;
                } else if (value > Float4Value.MAX_VALUE) {
                    throw new Error(`Float4 overflow: value ${value} exceeds maximum ${Float4Value.MAX_VALUE}`);
                } else if (value < Float4Value.MIN_VALUE) {
                    throw new Error(`Float4 underflow: value ${value} exceeds minimum ${Float4Value.MIN_VALUE}`);
                } else {
                    // Convert to Float32 precision
                    const float32Array = new Float32Array(1);
                    float32Array[0] = value;
                    this.value = float32Array[0];
                }
            }
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Float4Value {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Float4Value(undefined);
        }

        const num = Number(trimmed);

        if (Number.isNaN(num) && trimmed.toLowerCase() !== 'nan') {
            throw new Error(`Cannot parse "${str}" as Float4`);
        }

        return new Float4Value(num);
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