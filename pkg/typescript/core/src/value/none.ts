// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {BaseType, Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";

export class NoneValue implements Value {
    readonly type: Type = "None" as const;
    public readonly innerType: BaseType;

    constructor(innerType?: BaseType) {
        this.innerType = innerType ?? "None";
    }

    static parse(str: string, innerType?: BaseType): NoneValue {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === NONE_VALUE || trimmed === 'none') {
            return new NoneValue(innerType);
        }
        throw new Error(`Cannot parse "${str}" as None`);
    }

    isNone(): boolean {
        return true;
    }

    toString(): string {
        return 'none';
    }

    valueOf(): undefined {
        return undefined;
    }

    get value(): undefined {
        return undefined;
    }

    equals(other: Value): boolean {
        if (!(other instanceof NoneValue)) {
            return false;
        }
        return true;
    }

    compare(other: NoneValue): number {
        return 0;
    }

    encode(): TypeValuePair {
        return {
            type: "None",
            value: NONE_VALUE
        };
    }
}
