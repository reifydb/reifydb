import {Type, Value} from "./type";

export class Int16 implements Value {
    readonly type: Type = "Int16" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt("-170141183460469231731687303715884105728");
    private static readonly MAX_VALUE = BigInt("170141183460469231731687303715884105727");

    constructor(value?: bigint | number | string) {
        if (value !== undefined) {
            let bigintValue: bigint;
            
            if (typeof value === 'string') {
                try {
                    bigintValue = BigInt(value);
                } catch (e) {
                    throw new Error(`Int16 value must be a valid integer, got ${value}`);
                }
            } else if (typeof value === 'number') {
                bigintValue = BigInt(Math.trunc(value));
            } else {
                bigintValue = value;
            }
            
            if (bigintValue < Int16.MIN_VALUE || bigintValue > Int16.MAX_VALUE) {
                throw new Error(`Int16 value must be between ${Int16.MIN_VALUE} and ${Int16.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Int16 {
        const trimmed = str.trim();
        if (trimmed === '') {
            return new Int16(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Int16`);
        }
        
        if (value < Int16.MIN_VALUE || value > Int16.MAX_VALUE) {
            throw new Error(`Int16 value must be between ${Int16.MIN_VALUE} and ${Int16.MAX_VALUE}, got ${value}`);
        }
        
        return new Int16(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }
}