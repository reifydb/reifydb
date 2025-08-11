/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import type {
    Value,
    BoolValue,
    Int1Value,
    Int2Value,
    Int4Value,
    Int8Value,
    Int16Value,
    Uint1Value,
    Uint2Value,
    Uint4Value,
    Uint8Value,
    Uint16Value,
    Float4Value,
    Float8Value,
    Utf8Value,
    DateValue,
    DateTimeValue,
    TimeValue,
    IntervalValue,
    Uuid4Value,
    Uuid7Value,
    UndefinedValue,
    BlobValue,
    RowIdValue
} from '../value';

/**
 * Primitive types supported by the schema system
 */
export type PrimitiveType = 'string' | 'number' | 'boolean' | 'bigint' | 'Date' | 'undefined' | 'null';

/**
 * Value object types supported by the schema system
 */
export type ValueType = 
    | 'BoolValue'
    | 'Int1Value'
    | 'Int2Value'
    | 'Int4Value'
    | 'Int8Value'
    | 'Int16Value'
    | 'Uint1Value'
    | 'Uint2Value'
    | 'Uint4Value'
    | 'Uint8Value'
    | 'Uint16Value'
    | 'Float4Value'
    | 'Float8Value'
    | 'Utf8Value'
    | 'DateValue'
    | 'DateTimeValue'
    | 'TimeValue'
    | 'IntervalValue'
    | 'Uuid4Value'
    | 'Uuid7Value'
    | 'UndefinedValue'
    | 'BlobValue'
    | 'RowIdValue';

/**
 * Schema node types for defining data structures
 */
export type SchemaNode =
    | PrimitiveSchemaNode
    | ValueSchemaNode
    | AutoSchemaNode
    | ObjectSchemaNode
    | ArraySchemaNode
    | UnionSchemaNode
    | OptionalSchemaNode
    | TupleSchemaNode;

export interface PrimitiveSchemaNode {
    kind: 'primitive';
    type: PrimitiveType;
}

export interface ValueSchemaNode {
    kind: 'value';
    type: ValueType;
}

export interface AutoSchemaNode {
    kind: 'auto';
    hint?: PrimitiveType | 'integer' | 'float';
}

export interface ObjectSchemaNode {
    kind: 'object';
    properties: Record<string, SchemaNode>;
}

export interface ArraySchemaNode {
    kind: 'array';
    items: SchemaNode;
}

export interface TupleSchemaNode {
    kind: 'tuple';
    items: SchemaNode[];
}

export interface UnionSchemaNode {
    kind: 'union';
    types: SchemaNode[];
}

export interface OptionalSchemaNode {
    kind: 'optional';
    schema: SchemaNode;
}

/**
 * Bidirectional schema with separate param and result definitions
 */
export interface BidirectionalSchema<P extends SchemaNode = SchemaNode, R extends SchemaNode = SchemaNode> {
    params?: P;
    result?: R;
    validation?: {
        params?: (value: any) => boolean | string; // Return true or error message
        result?: (value: any) => boolean | string;
    };
}

/**
 * Map Value type names to their corresponding classes
 */
export interface ValueTypeMap {
    'BoolValue': BoolValue;
    'Int1Value': Int1Value;
    'Int2Value': Int2Value;
    'Int4Value': Int4Value;
    'Int8Value': Int8Value;
    'Int16Value': Int16Value;
    'Uint1Value': Uint1Value;
    'Uint2Value': Uint2Value;
    'Uint4Value': Uint4Value;
    'Uint8Value': Uint8Value;
    'Uint16Value': Uint16Value;
    'Float4Value': Float4Value;
    'Float8Value': Float8Value;
    'Utf8Value': Utf8Value;
    'DateValue': DateValue;
    'DateTimeValue': DateTimeValue;
    'TimeValue': TimeValue;
    'IntervalValue': IntervalValue;
    'Uuid4Value': Uuid4Value;
    'Uuid7Value': Uuid7Value;
    'UndefinedValue': UndefinedValue;
    'BlobValue': BlobValue;
    'RowIdValue': RowIdValue;
}

/**
 * Type inference for schema nodes
 */
export type InferSchemaType<S> = S extends PrimitiveSchemaNode
    ? S['type'] extends 'string' ? string
    : S['type'] extends 'number' ? number
    : S['type'] extends 'boolean' ? boolean
    : S['type'] extends 'bigint' ? bigint
    : S['type'] extends 'Date' ? Date
    : S['type'] extends 'undefined' ? undefined
    : S['type'] extends 'null' ? null
    : never
    : S extends ValueSchemaNode
    ? S['type'] extends keyof ValueTypeMap ? ValueTypeMap[S['type']]
    : Value
    : S extends ObjectSchemaNode
    ? { [K in keyof S['properties']]: InferSchemaType<S['properties'][K]> }
    : S extends ArraySchemaNode
    ? InferSchemaType<S['items']>[]
    : S extends TupleSchemaNode
    ? { [K in keyof S['items']]: InferSchemaType<S['items'][K]> }
    : S extends OptionalSchemaNode
    ? InferSchemaType<S['schema']> | undefined
    : S extends UnionSchemaNode
    ? InferSchemaType<S['types'][number]>
    : S extends AutoSchemaNode
    ? any
    : never;

/**
 * Type inference for bidirectional schemas
 */
export type InferParamType<S extends BidirectionalSchema> = S['params'] extends SchemaNode
    ? InferSchemaType<S['params']>
    : any;

export type InferResultType<S extends BidirectionalSchema> = S['result'] extends SchemaNode
    ? InferSchemaType<S['result']>
    : any;