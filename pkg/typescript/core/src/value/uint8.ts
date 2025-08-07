import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Uint8 implements Value {
    readonly type: Type = "Uint8" as const;
    public readonly value?: bigint;

    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("18446744073709551615");

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;
            
            if (bigintValue < Uint8.MIN_VALUE || bigintValue > Uint8.MAX_VALUE) {
                throw new Error(`Uint8 value must be between ${Uint8.MIN_VALUE} and ${Uint8.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Uint8 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint8(undefined);
        }
        
        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as Uint8`);
        }
        
        if (value < Uint8.MIN_VALUE || value > Uint8.MAX_VALUE) {
            throw new Error(`Uint8 value must be between ${Uint8.MIN_VALUE} and ${Uint8.MAX_VALUE}, got ${value}`);
        }
        
        return new Uint8(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    toNumber(): number | undefined {
        if (this.value === undefined) return undefined;
        return Number(this.value);
    }
}