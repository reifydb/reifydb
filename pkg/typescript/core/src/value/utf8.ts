import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Utf8 implements Value {
    readonly type: Type = "Utf8" as const;
    public readonly value?: string;

    constructor(value?: string) {
        if (value !== undefined) {
            if (typeof value !== 'string') {
                throw new Error(`Utf8 value must be a string, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Utf8 {
        if (str === UNDEFINED_VALUE) {
            return new Utf8(undefined);
        }
        
        return new Utf8(str);
    }

    valueOf(): string | undefined {
        return this.value;
    }
}