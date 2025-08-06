import {Type, Value} from "./type";

export class Int1 implements Value {
    readonly type: Type = "Int1" as const;

    constructor(public value?: number) {
    }

    valueOf(): number | undefined {
        return this.value;
    }
}