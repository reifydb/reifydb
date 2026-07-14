// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

// A dense array of f32. Mirrors VectorValue in crates/value/src/value/vector/mod.rs:
// Display is "[a, b, c]" with each element formatted like Float4, and the dimension
// (element count) is fixed per column by the vector(N) type constraint.
export class VectorValue implements Value {

    readonly type: Type = "Vector" as const;
    public readonly value?: Float32Array;

    constructor(value?: Float32Array | number[]) {
        if (value === undefined) {
            this.value = undefined;
            return;
        }

        if (value instanceof Float32Array) {
            this.value = value;
            return;
        }

        if (!Array.isArray(value)) {
            throw new Error(`Vector value must be a Float32Array or number[], got ${typeof value}`);
        }

        for (const element of value) {
            if (typeof element !== 'number') {
                throw new Error(`Vector elements must be numbers, got ${typeof element}`);
            }
        }

        this.value = Float32Array.from(value);
    }

    static parse(str: string): VectorValue {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE) {
            return new VectorValue(undefined);
        }

        if (!trimmed.startsWith('[') || !trimmed.endsWith(']')) {
            throw new Error(`Cannot parse "${str}" as Vector: expected a bracketed list`);
        }

        const inner = trimmed.slice(1, -1).trim();
        if (inner === '') {
            return new VectorValue(new Float32Array(0));
        }

        const elements = inner.split(',').map((part) => {
            const element = Number(part.trim());
            if (Number.isNaN(element)) {
                throw new Error(`Cannot parse "${part.trim()}" as a Vector element`);
            }
            return element;
        });

        return new VectorValue(elements);
    }

    dims(): number {
        return this.value === undefined ? 0 : this.value.length;
    }

    valueOf(): Float32Array | undefined {
        return this.value;
    }

    toString(): string {
        if (this.value === undefined) return 'none';
        return `[${Array.from(this.value, (element) => element.toString()).join(', ')}]`;
    }

    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }

        const otherVector = other as VectorValue;
        if (this.value === undefined || otherVector.value === undefined) {
            return this.value === otherVector.value;
        }

        if (this.value.length !== otherVector.value.length) {
            return false;
        }

        for (let i = 0; i < this.value.length; i++) {
            if (this.value[i] !== otherVector.value[i]) return false;
        }
        return true;
    }

    toJSON(): string[] | null {
        if (this.value === undefined) return null;
        return Array.from(this.value, (element) => element.toString());
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? NONE_VALUE : this.toString()
        };
    }

}
