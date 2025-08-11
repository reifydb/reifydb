/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {
    PrimitiveSchemaNode, ObjectSchemaNode, ArraySchemaNode,
    OptionalSchemaNode, SchemaNode
} from '.';

export class SchemaBuilder {
    static blob(): PrimitiveSchemaNode<'Blob'> {
        return {kind: 'primitive', type: 'Blob'};
    }

    static bool(): PrimitiveSchemaNode<'Bool'> {
        return {kind: 'primitive', type: 'Bool'};
    }

    static boolean(): PrimitiveSchemaNode<'Bool'> {
        return {kind: 'primitive', type: 'Bool'};
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

    static interval(): PrimitiveSchemaNode<'Interval'> {
        return {kind: 'primitive', type: 'Interval'};
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

    static rowid(): PrimitiveSchemaNode<'RowId'> {
        return {kind: 'primitive', type: 'RowId'};
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
}

export const Schema = SchemaBuilder;