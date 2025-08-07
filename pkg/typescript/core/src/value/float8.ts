import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Float8 implements Value {
    readonly type: Type = "Float8" as const;
    public readonly value?: number;

    constructor(value?: number) {
        if (value !== undefined) {
            if (typeof value !== 'number') {
                throw new Error(`Float8 value must be a number, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Float8 {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Float8(undefined);
        }

        const num = Number(trimmed);

        if (Number.isNaN(num) && trimmed.toLowerCase() !== 'nan') {
            throw new Error(`Cannot parse "${str}" as Float8`);
        }

        return new Float8(num);
    }

    valueOf(): number | undefined {
        return this.value;
    }
}
