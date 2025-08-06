/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Interval} from "./interval";
import {Type} from "./types";

// Tagged value types for explicit type specification
interface TypedValue<T extends Type, V> {
    type: T;
    value?: V;
}

export type BoolValue = TypedValue<"Bool", boolean | undefined>;
export type Float4Value = TypedValue<"Float4", number | undefined>;
export type Float8Value = TypedValue<"Float8", number | undefined>;
export type Int1Value = TypedValue<"Int1", number | undefined>;
export type Int2Value = TypedValue<"Int2", number | undefined>;
export type Int4Value = TypedValue<"Int4", number | undefined>;
export type Int8Value = TypedValue<"Int8", bigint | number | undefined>;
export type Int16Value = TypedValue<"Int16", bigint | number | undefined>;
export type Uint1Value = TypedValue<"Uint1", number | undefined>;
export type Uint2Value = TypedValue<"Uint2", number | undefined>;
export type Uint4Value = TypedValue<"Uint4", number | undefined>;
export type Uint8Value = TypedValue<"Uint8", bigint | number | undefined>;
export type Uint16Value = TypedValue<"Uint16", bigint | number | undefined>;
export type Utf8Value = TypedValue<"Utf8", string | undefined>;
export type DateValue = TypedValue<"Date", Date | undefined>;
export type DateTimeValue = TypedValue<"DateTime", Date | undefined>;
export type TimeValue = TypedValue<"Time", Date | undefined>;
export type IntervalValue = TypedValue<"Interval", Interval | undefined>;
export type Uuid4Value = TypedValue<"Uuid4", string | undefined>;
export type Uuid7Value = TypedValue<"Uuid7", string | undefined>;
export type UndefinedValue = TypedValue<"Undefined", undefined>;

// ReifyValue type that can be primitive or typed
export type ReifyValue =
    | boolean | number | bigint | string | Date | Interval | null | undefined
    | BoolValue
    | Float4Value
    | Float8Value
    | Int1Value
    | Int2Value
    | Int4Value
    | Int8Value
    | Int16Value
    | Uint1Value
    | Uint2Value
    | Uint4Value
    | Uint8Value
    | Uint16Value
    | Utf8Value
    | DateValue
    | DateTimeValue
    | TimeValue
    | IntervalValue
    | Uuid4Value
    | Uuid7Value
    | UndefinedValue;

// Factory class named "Value" with static methods
export class Value {
    static Bool(value: boolean): BoolValue {
        return {type: "Bool", value};
    }

    static Float4(value: number): Float4Value {
        return {type: "Float4", value};
    }

    static Float8(value: number): Float8Value {
        return {type: "Float8", value};
    }

    static Int1(value: number): Int1Value {
        return {type: "Int1", value};
    }

    static Int2(value: number): Int2Value {
        return {type: "Int2", value};
    }

    static Int4(value: number): Int4Value {
        return {type: "Int4", value};
    }

    static Int8(value: bigint | number): Int8Value {
        return {type: "Int8", value};
    }

    static Int16(value: bigint | number): Int16Value {
        return {type: "Int16", value};
    }

    static Uint1(value: number): Uint1Value {
        return {type: "Uint1", value};
    }

    static Uint2(value: number): Uint2Value {
        return {type: "Uint2", value};
    }

    static Uint4(value: number): Uint4Value {
        return {type: "Uint4", value};
    }

    static Uint8(value: bigint | number): Uint8Value {
        return {type: "Uint8", value};
    }

    static Uint16(value: bigint | number): Uint16Value {
        return {type: "Uint16", value};
    }

    static Utf8(value: string): Utf8Value {
        return {type: "Utf8", value};
    }

    static Date(value: Date): DateValue {
        return {type: "Date", value};
    }

    static DateTime(value: Date): DateTimeValue {
        return {type: "DateTime", value};
    }

    static Time(value: Date): TimeValue {
        return {type: "Time", value};
    }

    static Interval(value: Interval): IntervalValue {
        return {type: "Interval", value};
    }

    static Uuid4(value: string): Uuid4Value {
        return {type: "Uuid4", value};
    }

    static Uuid7(value: string): Uuid7Value {
        return {type: "Uuid7", value};
    }

    static Undefined(): UndefinedValue {
        return {type: "Undefined", value: undefined};
    }

}