// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {
    PrimitiveSchemaNode, ObjectSchemaNode, ArraySchemaNode,
    OptionalSchemaNode, ValueSchemaNode, SchemaNode
} from '.';

export class SchemaBuilder {

    static blob(): PrimitiveSchemaNode<'Blob'> {
        return {kind: 'primitive', type: 'Blob'};
    }

    static bool(): PrimitiveSchemaNode<'Boolean'> {
        return {kind: 'primitive', type: 'Boolean'};
    }

    static boolean(): PrimitiveSchemaNode<'Boolean'> {
        return {kind: 'primitive', type: 'Boolean'};
    }

    static decimal(): PrimitiveSchemaNode<'Decimal'> {
        return {kind: 'primitive', type: 'Decimal'};
    }

    static float4(): PrimitiveSchemaNode<'Float4'> {
        return {kind: 'primitive', type: 'Float4'};
    }

    static float8(): PrimitiveSchemaNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static float(): PrimitiveSchemaNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static double(): PrimitiveSchemaNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static int1(): PrimitiveSchemaNode<'Int1'> {
        return {kind: 'primitive', type: 'Int1'};
    }

    static int2(): PrimitiveSchemaNode<'Int2'> {
        return {kind: 'primitive', type: 'Int2'};
    }

    static int4(): PrimitiveSchemaNode<'Int4'> {
        return {kind: 'primitive', type: 'Int4'};
    }

    static int8(): PrimitiveSchemaNode<'Int8'> {
        return {kind: 'primitive', type: 'Int8'};
    }

    static int16(): PrimitiveSchemaNode<'Int16'> {
        return {kind: 'primitive', type: 'Int16'};
    }

    static int(): PrimitiveSchemaNode<'Int4'> {
        return {kind: 'primitive', type: 'Int4'};
    }

    static bigint(): PrimitiveSchemaNode<'Int8'> {
        return {kind: 'primitive', type: 'Int8'};
    }

    static uint1(): PrimitiveSchemaNode<'Uint1'> {
        return {kind: 'primitive', type: 'Uint1'};
    }

    static uint2(): PrimitiveSchemaNode<'Uint2'> {
        return {kind: 'primitive', type: 'Uint2'};
    }

    static uint4(): PrimitiveSchemaNode<'Uint4'> {
        return {kind: 'primitive', type: 'Uint4'};
    }

    static uint8(): PrimitiveSchemaNode<'Uint8'> {
        return {kind: 'primitive', type: 'Uint8'};
    }

    static uint16(): PrimitiveSchemaNode<'Uint16'> {
        return {kind: 'primitive', type: 'Uint16'};
    }

    static utf8(): PrimitiveSchemaNode<'Utf8'> {
        return {kind: 'primitive', type: 'Utf8'};
    }

    static string(): PrimitiveSchemaNode<'Utf8'> {
        return {kind: 'primitive', type: 'Utf8'};
    }

    static date(): PrimitiveSchemaNode<'Date'> {
        return {kind: 'primitive', type: 'Date'};
    }

    static datetime(): PrimitiveSchemaNode<'DateTime'> {
        return {kind: 'primitive', type: 'DateTime'};
    }

    static time(): PrimitiveSchemaNode<'Time'> {
        return {kind: 'primitive', type: 'Time'};
    }

    static duration(): PrimitiveSchemaNode<'Duration'> {
        return {kind: 'primitive', type: 'Duration'};
    }

    static uuid4(): PrimitiveSchemaNode<'Uuid4'> {
        return {kind: 'primitive', type: 'Uuid4'};
    }

    static uuid7(): PrimitiveSchemaNode<'Uuid7'> {
        return {kind: 'primitive', type: 'Uuid7'};
    }

    static uuid(): PrimitiveSchemaNode<'Uuid7'> {
        return {kind: 'primitive', type: 'Uuid7'};
    }

    static undefined(): PrimitiveSchemaNode<'Undefined'> {
        return {kind: 'primitive', type: 'Undefined'};
    }

    static identityid(): PrimitiveSchemaNode<'IdentityId'> {
        return {kind: 'primitive', type: 'IdentityId'};
    }

    static object<P extends Record<string, SchemaNode>>(properties: P): ObjectSchemaNode<P> {
        return {kind: 'object', properties};
    }

    static array<T extends SchemaNode>(items: T): ArraySchemaNode<T> {
        return {kind: 'array', items};
    }

    static optional<T extends SchemaNode>(schema: T): OptionalSchemaNode<T> {
        return {kind: 'optional', schema};
    }
    
    static number(): PrimitiveSchemaNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static booleanValue(): ValueSchemaNode<'Boolean'> {
        return {kind: 'value', type: 'Boolean'};
    }

    static decimalValue(): ValueSchemaNode<'Decimal'> {
        return {kind: 'value', type: 'Decimal'};
    }

    static int1Value(): ValueSchemaNode<'Int1'> {
        return {kind: 'value', type: 'Int1'};
    }

    static int2Value(): ValueSchemaNode<'Int2'> {
        return {kind: 'value', type: 'Int2'};
    }

    static int4Value(): ValueSchemaNode<'Int4'> {
        return {kind: 'value', type: 'Int4'};
    }

    static int8Value(): ValueSchemaNode<'Int8'> {
        return {kind: 'value', type: 'Int8'};
    }

    static int16Value(): ValueSchemaNode<'Int16'> {
        return {kind: 'value', type: 'Int16'};
    }

    static uint1Value(): ValueSchemaNode<'Uint1'> {
        return {kind: 'value', type: 'Uint1'};
    }

    static uint2Value(): ValueSchemaNode<'Uint2'> {
        return {kind: 'value', type: 'Uint2'};
    }

    static uint4Value(): ValueSchemaNode<'Uint4'> {
        return {kind: 'value', type: 'Uint4'};
    }

    static uint8Value(): ValueSchemaNode<'Uint8'> {
        return {kind: 'value', type: 'Uint8'};
    }

    static uint16Value(): ValueSchemaNode<'Uint16'> {
        return {kind: 'value', type: 'Uint16'};
    }

    static float4Value(): ValueSchemaNode<'Float4'> {
        return {kind: 'value', type: 'Float4'};
    }

    static float8Value(): ValueSchemaNode<'Float8'> {
        return {kind: 'value', type: 'Float8'};
    }

    static utf8Value(): ValueSchemaNode<'Utf8'> {
        return {kind: 'value', type: 'Utf8'};
    }

    static dateValue(): ValueSchemaNode<'Date'> {
        return {kind: 'value', type: 'Date'};
    }

    static dateTimeValue(): ValueSchemaNode<'DateTime'> {
        return {kind: 'value', type: 'DateTime'};
    }

    static timeValue(): ValueSchemaNode<'Time'> {
        return {kind: 'value', type: 'Time'};
    }

    static durationValue(): ValueSchemaNode<'Duration'> {
        return {kind: 'value', type: 'Duration'};
    }

    static uuid4Value(): ValueSchemaNode<'Uuid4'> {
        return {kind: 'value', type: 'Uuid4'};
    }

    static uuid7Value(): ValueSchemaNode<'Uuid7'> {
        return {kind: 'value', type: 'Uuid7'};
    }

    static undefinedValue(): ValueSchemaNode<'Undefined'> {
        return {kind: 'value', type: 'Undefined'};
    }

    static blobValue(): ValueSchemaNode<'Blob'> {
        return {kind: 'value', type: 'Blob'};
    }

    static identityIdValue(): ValueSchemaNode<'IdentityId'> {
        return {kind: 'value', type: 'IdentityId'};
    }
}

export const Schema = SchemaBuilder;