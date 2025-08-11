/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

export {BlobValue} from './blob';
export {BoolValue} from './bool';
export {DateValue} from './date';
export {DateTimeValue} from './datetime';
export {Float4Value} from './float4';
export {Float8Value} from './float8';
export {Int1Value} from './int1';
export {Int2Value} from './int2';
export {Int4Value} from './int4';
export {Int8Value} from './int8';
export {Int16Value} from './int16';
export {IntervalValue} from './interval';
export {TimeValue} from './time';
export {Uint1Value} from './uint1';
export {Uint2Value} from './uint2';
export {Uint4Value} from './uint4';
export {Uint8Value} from './uint8';
export {Uint16Value} from './uint16';
export {RowIdValue} from './rowid';
export {UndefinedValue} from './undefined';
export {Utf8Value} from './utf8';
export {Uuid4Value} from './uuid4';
export {Uuid7Value} from './uuid7';
// export {Type, Value, TypeValuePair} from './type';



export type Type =
    | "Blob"
    | "Bool"
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Utf8"
    | "Date" | "DateTime" | "Time" | "Interval"
    | "Uuid4" | "Uuid7"
    | "Undefined"
    | "RowId";

export interface TypeValuePair {
    type: Type;
    value: string;
}

export abstract class Value {
    abstract readonly type: Type;

    abstract encode(): TypeValuePair;
}