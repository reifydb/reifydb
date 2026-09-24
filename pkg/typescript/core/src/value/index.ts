// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
export {BlobValue} from './blob';
export {BooleanValue} from './boolean';
export {DateValue} from './date';
export {DateTimeValue} from './datetime';
export {DecimalValue} from './decimal';
export {DigestValue, digestType, digestTypeName} from './digest';
export {
    DECIMAL_DEFAULT_SCALE, FIXED_POINT_MAX_PRECISION, FIXED_POINT_NARROW_PRECISION,
    decimalType, fixedPointKind, fixedPointPrecision, fixedPointScale, fixedPointType, fixedPointTypeName,
    intType, isFixedPointType, uintType,
} from './fixed-point';
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
export {NoneValue, noneDepth} from './none';
export {Option, isOption} from './option';
export {Utf8Value} from './utf8';
export {Uuid4Value} from './uuid4';
export {Uuid7Value} from './uuid7';
export {IdentityIdValue} from './identityid';
export {ListValue} from './list';
export {RecordValue} from './record';
export type {WireType} from './wire-type';
export {typeToWire, typeFromWire, columnsFromWire, framesFromWire, envelopeToColumns, envelopesToFrames} from './wire-type';

export type BaseType =
    | "Blob"
    | "Boolean"
    | "Decimal"
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Utf8"
    | "Date" | "DateTime" | "Time" | "Duration"
    | "Uuid4" | "Uuid7"
    | "IdentityId"
    | "None";

export interface OptionType { Option: Type }
export type DigestInnerType =
    | "Float4" | "Float8"
    | "Int1" | "Int2" | "Int4" | "Int8" | "Int16"
    | "Uint1" | "Uint2" | "Uint4" | "Uint8" | "Uint16"
    | "Duration"
    | "Int" | "Uint";

export interface DigestType { Digest: { inner: DigestInnerType; accuracy: number } }
export type FixedPointKind = "Int" | "Uint" | "Decimal";
export interface IntType { Int: { precision: number } }
export interface UintType { Uint: { precision: number } }
export interface DecimalType { Decimal: { precision: number; scale: number } }
export type FixedPointType = IntType | UintType | DecimalType;
export interface RecordField { name: string; type: Type }
export interface ListType { List: Type }
export interface RecordType { Record: RecordField[] }
export type Type = BaseType | OptionType | DigestType | FixedPointType | ListType | RecordType;

export function isOptionType(t: Type): t is OptionType {
    return typeof t === 'object' && t !== null && 'Option' in t;
}

export function isDigestType(t: Type): t is DigestType {
    return typeof t === 'object' && t !== null && 'Digest' in t;
}

export function isListType(t: Type): t is ListType {
    return typeof t === 'object' && t !== null && 'List' in t;
}

export function isRecordType(t: Type): t is RecordType {
    return typeof t === 'object' && t !== null && 'Record' in t;
}

export function unwrapOptionType(t: Type): BaseType | DigestType | FixedPointType | ListType | RecordType {
    if (isOptionType(t)) return unwrapOptionType(t.Option);
    return t;
}

export function optionDepth(t: Type): number {
    return isOptionType(t) ? optionDepth(t.Option) + 1 : 0;
}

export function innerOfOption(t: OptionType): Type {
    return t.Option;
}

/** A wire cell: a plain string for every scalar type, or a real JSON array/object for List/Record. */
export type WireCellValue = string | WireCellValue[] | {[key: string]: WireCellValue};

export interface TypeValuePair {
    type: Type;
    value: WireCellValue;
}

export abstract class Value {
    abstract readonly type: Type;

    public abstract encode(): TypeValuePair;
    public abstract equals(other: Value): boolean;
}
