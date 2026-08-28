// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    BlobValue, BooleanValue, DateValue, DateTimeValue, DecimalValue,
    Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    DurationValue, TimeValue,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    NoneValue, Utf8Value,
    Uuid4Value, Uuid7Value, IdentityIdValue,
    BaseType
} from '../value';
import {
    PrimitiveShapeNode, ObjectShapeNode, ArrayShapeNode,
    OptionalShapeNode, ValueShapeNode, ShapeNode
} from '.';

export interface PrimitiveTSMap {
    Blob: Uint8Array;
    Boolean: boolean;
    Decimal: string;
    Float4: number;
    Float8: number;
    Int1: number;
    Int2: number;
    Int4: number;
    Int8: bigint;
    Int16: bigint;
    Uint1: number;
    Uint2: number;
    Uint4: number;
    Uint8: bigint;
    Uint16: bigint;
    Utf8: string;
    Date: Date;
    DateTime: Date;
    Time: string;
    Duration: string;
    Uuid4: string;
    Uuid7: string;
    None: undefined;
    IdentityId: string;
}

export interface PrimitiveValueMap {
    Blob: BlobValue;
    Boolean: BooleanValue;
    Decimal: DecimalValue;
    Float4: Float4Value;
    Float8: Float8Value;
    Int1: Int1Value;
    Int2: Int2Value;
    Int4: Int4Value;
    Int8: Int8Value;
    Int16: Int16Value;
    Uint1: Uint1Value;
    Uint2: Uint2Value;
    Uint4: Uint4Value;
    Uint8: Uint8Value;
    Uint16: Uint16Value;
    Utf8: Utf8Value;
    Date: DateValue;
    DateTime: DateTimeValue;
    Time: TimeValue;
    Duration: DurationValue;
    Uuid4: Uuid4Value;
    Uuid7: Uuid7Value;
    None: NoneValue;
    IdentityId: IdentityIdValue;
}

export type PrimitiveToTS<T extends BaseType> = PrimitiveTSMap[T];

export type PrimitiveToValue<T extends BaseType> = PrimitiveValueMap[T];

export type CamelCase<S extends string> =
    S extends `${infer Head}_${infer Tail}`
        ? `${Head}${Capitalize<CamelCase<Tail>>}`
        : S;

export type InferShape<S> =
    S extends PrimitiveShapeNode<infer T> ? T extends BaseType ? PrimitiveToTS<T> : never :
        S extends ValueShapeNode<infer T> ? T extends BaseType ? PrimitiveToValue<T> : never :
            S extends ObjectShapeNode<infer P> ? { [K in keyof P as CamelCase<K & string>]: InferShape<P[K]> } :
                S extends ArrayShapeNode<infer T> ? InferShape<T>[] :
                    S extends OptionalShapeNode<infer T> ? InferShape<T> | undefined :
                        never;

export type InferShapes<S extends readonly ShapeNode[]> = {
    [K in keyof S]: InferShape<S[K]>[]
};
