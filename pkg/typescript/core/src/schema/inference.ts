/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {
    BlobValue, BoolValue, DateValue, DateTimeValue,
    Float4Value, Float8Value,
    Int1Value, Int2Value, Int4Value, Int8Value, Int16Value,
    IntervalValue, TimeValue,
    Uint1Value, Uint2Value, Uint4Value, Uint8Value, Uint16Value,
    RowIdValue, UndefinedValue, Utf8Value,
    Uuid4Value, Uuid7Value, IdentityIdValue,
    Type
} from '../value';
import {
    PrimitiveSchemaNode, ObjectSchemaNode, ArraySchemaNode,
    OptionalSchemaNode, ValueSchemaNode, SchemaNode
} from '.';

export type PrimitiveToTS<T extends Type> =
    T extends 'Blob' ? Uint8Array :
        T extends 'Bool' ? boolean :
            T extends 'Float4' ? number :
                T extends 'Float8' ? number :
                    T extends 'Int1' ? number :
                        T extends 'Int2' ? number :
                            T extends 'Int4' ? number :
                                T extends 'Int8' ? bigint :
                                    T extends 'Int16' ? bigint :
                                        T extends 'Uint1' ? number :
                                            T extends 'Uint2' ? number :
                                                T extends 'Uint4' ? number :
                                                    T extends 'Uint8' ? bigint :
                                                        T extends 'Uint16' ? bigint :
                                                            T extends 'Utf8' ? string :
                                                                T extends 'Date' ? Date :
                                                                    T extends 'DateTime' ? Date :
                                                                        T extends 'Time' ? string :
                                                                            T extends 'Interval' ? string :
                                                                                T extends 'Uuid4' ? string :
                                                                                    T extends 'Uuid7' ? string :
                                                                                        T extends 'Undefined' ? undefined :
                                                                                            T extends 'RowId' ? bigint :
                                                                                                T extends 'IdentityId' ? string :
                                                                                                    never;

export type PrimitiveToValue<T extends Type> =
    T extends 'Blob' ? BlobValue :
        T extends 'Bool' ? BoolValue :
            T extends 'Float4' ? Float4Value :
                T extends 'Float8' ? Float8Value :
                    T extends 'Int1' ? Int1Value :
                        T extends 'Int2' ? Int2Value :
                            T extends 'Int4' ? Int4Value :
                                T extends 'Int8' ? Int8Value :
                                    T extends 'Int16' ? Int16Value :
                                        T extends 'Uint1' ? Uint1Value :
                                            T extends 'Uint2' ? Uint2Value :
                                                T extends 'Uint4' ? Uint4Value :
                                                    T extends 'Uint8' ? Uint8Value :
                                                        T extends 'Uint16' ? Uint16Value :
                                                            T extends 'Utf8' ? Utf8Value :
                                                                T extends 'Date' ? DateValue :
                                                                    T extends 'DateTime' ? DateTimeValue :
                                                                        T extends 'Time' ? TimeValue :
                                                                            T extends 'Interval' ? IntervalValue :
                                                                                T extends 'Uuid4' ? Uuid4Value :
                                                                                    T extends 'Uuid7' ? Uuid7Value :
                                                                                        T extends 'Undefined' ? UndefinedValue :
                                                                                            T extends 'RowId' ? RowIdValue :
                                                                                                T extends 'IdentityId' ? IdentityIdValue :
                                                                                                    never;

export type InferSchema<S> =
    S extends PrimitiveSchemaNode<infer T> ? T extends Type ? PrimitiveToTS<T> : never :
        S extends ValueSchemaNode<infer T> ? T extends Type ? PrimitiveToValue<T> : never :
            S extends ObjectSchemaNode<infer P> ? { [K in keyof P]: InferSchema<P[K]> } :
                S extends ArraySchemaNode<infer T> ? InferSchema<T>[] :
                    S extends OptionalSchemaNode<infer T> ? InferSchema<T> | undefined :
                        never;

export type InferSchemas<S extends readonly SchemaNode[]> = {
    [K in keyof S]: InferSchema<S[K]>[]
};