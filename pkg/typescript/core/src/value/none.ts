// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair, optionDepth} from ".";
import {NONE_VALUE} from "../constant";

export class NoneValue implements Value {
    readonly type: Type = "None" as const;
    public readonly innerType: Type;

    constructor(innerType?: Type) {
        this.innerType = innerType ?? "None";
    }

    static parse(str: string, innerType?: Type): NoneValue {
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

    toJSON(): null {
        return null;
    }

    encode(): TypeValuePair {
        return {
            type: {Option: this.innerType},
            value: NONE_VALUE
        };
    }
}

export function noneDepth(v: NoneValue): number {
    return optionDepth(v.innerType) + 1;
}
