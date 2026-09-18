// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {ListType, Type, TypeValuePair, Value} from '.';

function hasToJSON(value: unknown): value is {toJSON(): unknown} {
    return typeof value === 'object' && value !== null && 'toJSON' in value && typeof (value as {toJSON: unknown}).toJSON === 'function';
}

export class ListValue implements Value {
    public readonly items: Value[];
    private readonly elementType?: Type;

    constructor(items: Value[], elementType?: Type) {
        if (!Array.isArray(items)) {
            throw new Error(`List value must be an array, got ${typeof items}`);
        }
        if (items.length === 0 && elementType === undefined) {
            throw new Error('An empty List has no element type to infer, pass it explicitly: new ListValue([], elementType)');
        }
        this.items = items;
        this.elementType = elementType;
    }

    get type(): ListType {
        return {List: this.elementType ?? this.items[0].type};
    }

    equals(other: Value): boolean {
        if (!(other instanceof ListValue) || this.items.length !== other.items.length) {
            return false;
        }
        return this.items.every((item, i) => item.equals(other.items[i]));
    }

    toString(): string {
        return `[${this.items.map(item => item.toString()).join(', ')}]`;
    }

    toJSON(): unknown {
        return this.items.map(item => (hasToJSON(item) ? item.toJSON() : item));
    }

    encode(): TypeValuePair {
        const elementType = this.elementType ?? this.items[0].type;
        return {
            type: {List: elementType},
            value: this.items.map(item => item.encode().value),
        };
    }
}
