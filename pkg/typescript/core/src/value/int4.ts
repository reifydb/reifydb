import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Int4 implements Value {
    readonly type: Type = "Int4" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -2147483648;
    private static readonly MAX_VALUE = 2147483647;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int4 value must be an integer, got ${value}`);
            }
            if (value < Int4.MIN_VALUE || value > Int4.MAX_VALUE) {
                throw new Error(`Int4 value must be between ${Int4.MIN_VALUE} and ${Int4.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int4 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int4(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int4`);
        }
        
        return new Int4(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}