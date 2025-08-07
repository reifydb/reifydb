import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class Bool implements Value {
    readonly type: Type = "Bool" as const;
    public readonly value?: boolean;

    constructor(value?: boolean) {
        if (value !== undefined) {
            if (typeof value !== 'boolean') {
                throw new Error(`Bool value must be a boolean, got ${typeof value}`);
            }
            this.value = value;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): Bool {
        const trimmed = str.trim().toLowerCase();

        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new Bool(undefined);
        }

        if (trimmed === 'true') {
            return new Bool(true);
        }

        if (trimmed === 'false') {
            return new Bool(false);
        }

        throw new Error(`Cannot parse "${str}" as Bool`);
    }

    valueOf(): boolean | undefined {
        return this.value;
    }
}