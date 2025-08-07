import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Uint1 implements Value {
    readonly type: Type = "Uint1" as const;
    public readonly value?: number;

    private static readonly MIN_VALUE = 0;
    private static readonly MAX_VALUE = 255;

    constructor(value?: number) {
        if (value !== undefined) {
            if (!Number.isInteger(value)) {
                throw new Error(`Uint1 value must be an integer, got ${value}`);
            }
            if (value < Uint1.MIN_VALUE || value > Uint1.MAX_VALUE) {
                throw new Error(`Uint1 value must be between ${Uint1.MIN_VALUE} and ${Uint1.MAX_VALUE}, got ${value}`);
            }
        }
        this.value = value;
    }

    static parse(str: string): Uint1 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Uint1(undefined);
        }
        
        const num = Number(trimmed);
        
        if (isNaN(num)) {
            throw new Error(`Cannot parse "${str}" as Uint1`);
        }
        
        return new Uint1(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}