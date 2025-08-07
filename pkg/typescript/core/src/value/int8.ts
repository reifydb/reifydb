import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Int8 implements Value {
    readonly type: Type = "Int8" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt("-9223372036854775808");
    private static readonly MAX_VALUE = BigInt("9223372036854775807");

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;
            
            if (bigintValue < Int8.MIN_VALUE || bigintValue > Int8.MAX_VALUE) {
                throw new Error(`Int8 value must be between ${Int8.MIN_VALUE} and ${Int8.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Int8 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int8(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Int8`);
        }
        
        if (value < Int8.MIN_VALUE || value > Int8.MAX_VALUE) {
            throw new Error(`Int8 value must be between ${Int8.MIN_VALUE} and ${Int8.MAX_VALUE}, got ${value}`);
        }
        
        return new Int8(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }
}