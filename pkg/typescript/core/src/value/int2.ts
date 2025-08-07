import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Int2 implements Value {
    readonly type: Type = "Int2" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -32768;
    private static readonly MAX_VALUE = 32767;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int2 value must be an integer, got ${value}`);
            }
            if (value < Int2.MIN_VALUE || value > Int2.MAX_VALUE) {
                throw new Error(`Int2 value must be between ${Int2.MIN_VALUE} and ${Int2.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int2 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int2(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int2`);
        }
        
        return new Int2(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}