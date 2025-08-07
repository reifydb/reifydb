import {Type, Value} from "./type";
import {UNDEFINED_VALUE} from "../constant";

export class RowId implements Value {
    private static readonly MIN_VALUE = BigInt(0);
    private static readonly MAX_VALUE = BigInt("18446744073709551615");
    readonly type: Type = "RowId" as const;
    public readonly value?: bigint;

    constructor(value?: bigint | number) {
        if (value !== undefined) {
            const bigintValue = typeof value === 'number' ? BigInt(Math.trunc(value)) : value;

            if (bigintValue < RowId.MIN_VALUE || bigintValue > RowId.MAX_VALUE) {
                throw new Error(`RowId value must be between ${RowId.MIN_VALUE} and ${RowId.MAX_VALUE}, got ${bigintValue}`);
            }
            this.value = bigintValue;
        } else {
            this.value = undefined;
        }
    }

    static parse(str: string): RowId {
        const trimmed = str.trim();
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new RowId(undefined);
        }

        let value: bigint;
        try {
            value = BigInt(trimmed);
        } catch (e) {
            throw new Error(`Cannot parse "${str}" as RowId`);
        }

        if (value < RowId.MIN_VALUE || value > RowId.MAX_VALUE) {
            throw new Error(`RowId value must be between ${RowId.MIN_VALUE} and ${RowId.MAX_VALUE}, got ${value}`);
        }

        return new RowId(value);
    }

    valueOf(): bigint | undefined {
        return this.value;
    }
}