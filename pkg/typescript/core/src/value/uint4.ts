import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Uint4 implements Value {
    readonly type: Type = "Uint4" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 4294967295;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Uint4 value must be an integer, got ${value}`);
            }
            if (value < Uint4.MIN_VALUE || value > Uint4.MAX_VALUE) {
                throw new Error(`Uint4 value must be between ${Uint4.MIN_VALUE} and ${Uint4.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Uint4 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint4(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint4`);
        }
        
        return new Uint4(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}