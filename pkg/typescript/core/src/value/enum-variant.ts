// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Type, Value, TypeValuePair} from ".";

export class EnumVariant implements Value {
    readonly type: Type;
    readonly enum: string;
    readonly variant: string;

    constructor(enumName: string, variant: string) {
        if (typeof enumName !== 'string' || typeof variant !== 'string') {
            throw new Error(`EnumVariant needs a string enum and variant, got ${typeof enumName} and ${typeof variant}`);
        }
        this.type = {Enum: enumName};
        this.enum = enumName;
        this.variant = variant;
    }

    valueOf(): string {
        return this.variant;
    }

    toString(): string {
        return this.variant;
    }

    equals(other: Value): boolean {
        return other instanceof EnumVariant && other.enum === this.enum && other.variant === this.variant;
    }

    toJSON(): string {
        return this.variant;
    }

    encode(): TypeValuePair {
        return {type: this.type, value: this.variant};
    }
}
