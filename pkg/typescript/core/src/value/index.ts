/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

export {BlobValue} from './blob';
export {BooleanValue} from './boolean';
export {DateValue} from './date';
export {DateTimeValue} from './datetime';
export {Float4Value} from './float4';
export {Float8Value} from './float8';
export {Int1Value} from './int1';
export {Int2Value} from './int2';
export {Int4Value} from './int4';
export {Int8Value} from './int8';
export {Int16Value} from './int16';
export {DurationValue} from './duration';
export {TimeValue} from './time';
export {Uint1Value} from './uint1';
export {Uint2Value} from './uint2';
export {Uint4Value} from './uint4';
export {Uint8Value} from './uint8';
export {Uint16Value} from './uint16';
export {RowNumberValue} from './rownumber';
export {UndefinedValue} from './undefined';
export {Utf8Value} from './utf8';
export {Uuid4Value} from './uuid4';
export {Uuid7Value} from './uuid7';
export {IdentityIdValue} from './identityid';

export type Type =
    | "Blob"
    | "Boolean"
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Utf8"
    | "Date" | "DateTime" | "Time" | "Duration"
    | "Uuid4" | "Uuid7"
    | "Undefined"
    | "RowNumber"
    | "IdentityId";

export interface TypeValuePair {
    type: Type;
    value: string;
}

export abstract class Value {
    abstract readonly type: Type;

    public abstract encode(): TypeValuePair;
    public abstract equals(other: Value): boolean;
}