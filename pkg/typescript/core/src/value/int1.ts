import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Int1 implements Value {
    readonly type: Type = "Int1" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = -128;
    private static readonly MAX_VALUE = 127;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Int1 value must be an integer, got ${value}`);
            }
            if (value < Int1.MIN_VALUE || value > Int1.MAX_VALUE) {
                throw new Error(`Int1 value must be between ${Int1.MIN_VALUE} and ${Int1.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Int1 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Int1(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Int1`);
        }
        
        return new Int1(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}