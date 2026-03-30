// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {
    PrimitiveShapeNode, ObjectShapeNode, ArrayShapeNode,
    OptionalShapeNode, ValueShapeNode, ShapeNode
} from '.';

export class ShapeBuilder {

    static blob(): PrimitiveShapeNode<'Blob'> {
        return {kind: 'primitive', type: 'Blob'};
    }

    static bool(): PrimitiveShapeNode<'Boolean'> {
        return {kind: 'primitive', type: 'Boolean'};
    }

    static boolean(): PrimitiveShapeNode<'Boolean'> {
        return {kind: 'primitive', type: 'Boolean'};
    }

    static decimal(): PrimitiveShapeNode<'Decimal'> {
        return {kind: 'primitive', type: 'Decimal'};
    }

    static float4(): PrimitiveShapeNode<'Float4'> {
        return {kind: 'primitive', type: 'Float4'};
    }

    static float8(): PrimitiveShapeNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static float(): PrimitiveShapeNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static double(): PrimitiveShapeNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static int1(): PrimitiveShapeNode<'Int1'> {
        return {kind: 'primitive', type: 'Int1'};
    }

    static int2(): PrimitiveShapeNode<'Int2'> {
        return {kind: 'primitive', type: 'Int2'};
    }

    static int4(): PrimitiveShapeNode<'Int4'> {
        return {kind: 'primitive', type: 'Int4'};
    }

    static int8(): PrimitiveShapeNode<'Int8'> {
        return {kind: 'primitive', type: 'Int8'};
    }

    static int16(): PrimitiveShapeNode<'Int16'> {
        return {kind: 'primitive', type: 'Int16'};
    }

    static int(): PrimitiveShapeNode<'Int4'> {
        return {kind: 'primitive', type: 'Int4'};
    }

    static bigint(): PrimitiveShapeNode<'Int8'> {
        return {kind: 'primitive', type: 'Int8'};
    }

    static uint1(): PrimitiveShapeNode<'Uint1'> {
        return {kind: 'primitive', type: 'Uint1'};
    }

    static uint2(): PrimitiveShapeNode<'Uint2'> {
        return {kind: 'primitive', type: 'Uint2'};
    }

    static uint4(): PrimitiveShapeNode<'Uint4'> {
        return {kind: 'primitive', type: 'Uint4'};
    }

    static uint8(): PrimitiveShapeNode<'Uint8'> {
        return {kind: 'primitive', type: 'Uint8'};
    }

    static uint16(): PrimitiveShapeNode<'Uint16'> {
        return {kind: 'primitive', type: 'Uint16'};
    }

    static utf8(): PrimitiveShapeNode<'Utf8'> {
        return {kind: 'primitive', type: 'Utf8'};
    }

    static string(): PrimitiveShapeNode<'Utf8'> {
        return {kind: 'primitive', type: 'Utf8'};
    }

    static date(): PrimitiveShapeNode<'Date'> {
        return {kind: 'primitive', type: 'Date'};
    }

    static datetime(): PrimitiveShapeNode<'DateTime'> {
        return {kind: 'primitive', type: 'DateTime'};
    }

    static time(): PrimitiveShapeNode<'Time'> {
        return {kind: 'primitive', type: 'Time'};
    }

    static duration(): PrimitiveShapeNode<'Duration'> {
        return {kind: 'primitive', type: 'Duration'};
    }

    static uuid4(): PrimitiveShapeNode<'Uuid4'> {
        return {kind: 'primitive', type: 'Uuid4'};
    }

    static uuid7(): PrimitiveShapeNode<'Uuid7'> {
        return {kind: 'primitive', type: 'Uuid7'};
    }

    static uuid(): PrimitiveShapeNode<'Uuid7'> {
        return {kind: 'primitive', type: 'Uuid7'};
    }

    static none(): PrimitiveShapeNode<'None'> {
        return {kind: 'primitive', type: 'None'};
    }

    static identityid(): PrimitiveShapeNode<'IdentityId'> {
        return {kind: 'primitive', type: 'IdentityId'};
    }

    static object<P extends Record<string, ShapeNode>>(properties: P): ObjectShapeNode<P> {
        return {kind: 'object', properties};
    }

    static array<T extends ShapeNode>(items: T): ArrayShapeNode<T> {
        return {kind: 'array', items};
    }

    static optional<T extends ShapeNode>(shape: T): OptionalShapeNode<T> {
        return {kind: 'optional', shape};
    }

    static number(): PrimitiveShapeNode<'Float8'> {
        return {kind: 'primitive', type: 'Float8'};
    }

    static booleanValue(): ValueShapeNode<'Boolean'> {
        return {kind: 'value', type: 'Boolean'};
    }

    static decimalValue(): ValueShapeNode<'Decimal'> {
        return {kind: 'value', type: 'Decimal'};
    }

    static int1Value(): ValueShapeNode<'Int1'> {
        return {kind: 'value', type: 'Int1'};
    }

    static int2Value(): ValueShapeNode<'Int2'> {
        return {kind: 'value', type: 'Int2'};
    }

    static int4Value(): ValueShapeNode<'Int4'> {
        return {kind: 'value', type: 'Int4'};
    }

    static int8Value(): ValueShapeNode<'Int8'> {
        return {kind: 'value', type: 'Int8'};
    }

    static int16Value(): ValueShapeNode<'Int16'> {
        return {kind: 'value', type: 'Int16'};
    }

    static uint1Value(): ValueShapeNode<'Uint1'> {
        return {kind: 'value', type: 'Uint1'};
    }

    static uint2Value(): ValueShapeNode<'Uint2'> {
        return {kind: 'value', type: 'Uint2'};
    }

    static uint4Value(): ValueShapeNode<'Uint4'> {
        return {kind: 'value', type: 'Uint4'};
    }

    static uint8Value(): ValueShapeNode<'Uint8'> {
        return {kind: 'value', type: 'Uint8'};
    }

    static uint16Value(): ValueShapeNode<'Uint16'> {
        return {kind: 'value', type: 'Uint16'};
    }

    static float4Value(): ValueShapeNode<'Float4'> {
        return {kind: 'value', type: 'Float4'};
    }

    static float8Value(): ValueShapeNode<'Float8'> {
        return {kind: 'value', type: 'Float8'};
    }

    static utf8Value(): ValueShapeNode<'Utf8'> {
        return {kind: 'value', type: 'Utf8'};
    }

    static dateValue(): ValueShapeNode<'Date'> {
        return {kind: 'value', type: 'Date'};
    }

    static dateTimeValue(): ValueShapeNode<'DateTime'> {
        return {kind: 'value', type: 'DateTime'};
    }

    static timeValue(): ValueShapeNode<'Time'> {
        return {kind: 'value', type: 'Time'};
    }

    static durationValue(): ValueShapeNode<'Duration'> {
        return {kind: 'value', type: 'Duration'};
    }

    static uuid4Value(): ValueShapeNode<'Uuid4'> {
        return {kind: 'value', type: 'Uuid4'};
    }

    static uuid7Value(): ValueShapeNode<'Uuid7'> {
        return {kind: 'value', type: 'Uuid7'};
    }

    static noneValue(): ValueShapeNode<'None'> {
        return {kind: 'value', type: 'None'};
    }

    static blobValue(): ValueShapeNode<'Blob'> {
        return {kind: 'value', type: 'Blob'};
    }

    static identityIdValue(): ValueShapeNode<'IdentityId'> {
        return {kind: 'value', type: 'IdentityId'};
    }
}

export const Shape = ShapeBuilder;
