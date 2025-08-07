import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Uint2 implements Value {
    readonly type: Type = "Uint2" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 65535;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Uint2 value must be an integer, got ${value}`);
            }
            if (value < Uint2.MIN_VALUE || value > Uint2.MAX_VALUE) {
                throw new Error(`Uint2 value must be between ${Uint2.MIN_VALUE} and ${Uint2.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Uint2 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint2(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint2`);
        }
        
        return new Uint2(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}