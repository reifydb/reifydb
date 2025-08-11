/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import type {
    SchemaNode,
    PrimitiveSchemaNode,
    ValueSchemaNode,
    AutoSchemaNode,
    ObjectSchemaNode,
    ArraySchemaNode,
    TupleSchemaNode,
    UnionSchemaNode,
    OptionalSchemaNode,
    BidirectionalSchema,
    PrimitiveType,
    ValueType
} from './types';

/**
 * Fluent API for building schemas
 */
export class Schema {
    // Primitive type builders
    static string(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'string' };
    }

    static number(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'number' };
    }

    static boolean(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'boolean' };
    }

    static bigint(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'bigint' };
    }

    static date(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'Date' };
    }

    static undefined(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'undefined' };
    }

    static null(): PrimitiveSchemaNode {
        return { kind: 'primitive', type: 'null' };
    }

    // Value object builders
    static boolValue(): ValueSchemaNode {
        return { kind: 'value', type: 'BoolValue' };
    }

    static int1Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Int1Value' };
    }

    static int2Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Int2Value' };
    }

    static int4Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Int4Value' };
    }

    static int8Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Int8Value' };
    }

    static int16Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Int16Value' };
    }

    static uint1Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uint1Value' };
    }

    static uint2Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uint2Value' };
    }

    static uint4Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uint4Value' };
    }

    static uint8Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uint8Value' };
    }

    static uint16Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uint16Value' };
    }

    static float4Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Float4Value' };
    }

    static float8Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Float8Value' };
    }

    static utf8Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Utf8Value' };
    }

    static dateValue(): ValueSchemaNode {
        return { kind: 'value', type: 'DateValue' };
    }

    static dateTimeValue(): ValueSchemaNode {
        return { kind: 'value', type: 'DateTimeValue' };
    }

    static timeValue(): ValueSchemaNode {
        return { kind: 'value', type: 'TimeValue' };
    }

    static intervalValue(): ValueSchemaNode {
        return { kind: 'value', type: 'IntervalValue' };
    }

    static uuid4Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uuid4Value' };
    }

    static uuid7Value(): ValueSchemaNode {
        return { kind: 'value', type: 'Uuid7Value' };
    }

    static undefinedValue(): ValueSchemaNode {
        return { kind: 'value', type: 'UndefinedValue' };
    }

    static blobValue(): ValueSchemaNode {
        return { kind: 'value', type: 'BlobValue' };
    }

    static rowIdValue(): ValueSchemaNode {
        return { kind: 'value', type: 'RowIdValue' };
    }

    // Composite builders
    static object<T extends Record<string, SchemaNode>>(properties: T): ObjectSchemaNode {
        return { kind: 'object', properties };
    }

    static array<T extends SchemaNode>(items: T): ArraySchemaNode {
        return { kind: 'array', items };
    }

    static tuple<T extends SchemaNode[]>(...items: T): TupleSchemaNode {
        return { kind: 'tuple', items };
    }

    static union<T extends SchemaNode[]>(...types: T): UnionSchemaNode {
        return { kind: 'union', types };
    }

    static optional<T extends SchemaNode>(schema: T): OptionalSchemaNode {
        return { kind: 'optional', schema };
    }

    // Auto-detection
    static auto(hint?: PrimitiveType | 'integer' | 'float'): AutoSchemaNode {
        return { kind: 'auto', hint };
    }

    // Bidirectional schema builder
    static bidirectional<P extends SchemaNode, R extends SchemaNode>(config: {
        params?: P;
        result?: R;
        validation?: {
            params?: (value: any) => boolean | string;
            result?: (value: any) => boolean | string;
        };
    }): BidirectionalSchema<P, R> {
        return config;
    }
}

/**
 * Shorthand aliases for common patterns
 */
export const S = Schema; // Short alias

// Common composite patterns
export class SchemaPatterns {
    /**
     * Nullable schema (union of T and null)
     */
    static nullable<T extends SchemaNode>(schema: T): UnionSchemaNode {
        return Schema.union(schema, Schema.null());
    }

    /**
     * Array of primitives
     */
    static stringArray(): ArraySchemaNode {
        return Schema.array(Schema.string());
    }

    static numberArray(): ArraySchemaNode {
        return Schema.array(Schema.number());
    }

    static booleanArray(): ArraySchemaNode {
        return Schema.array(Schema.boolean());
    }

    /**
     * Common database field patterns
     */
    static id(): PrimitiveSchemaNode | ValueSchemaNode {
        return Schema.number(); // or Schema.int4Value() for Value object
    }

    static uuid(): ValueSchemaNode {
        return Schema.uuid7Value();
    }

    static timestamp(): PrimitiveSchemaNode {
        return Schema.date();
    }

    static email(): PrimitiveSchemaNode {
        return Schema.string();
    }

    /**
     * Pagination schema
     */
    static pagination() {
        return Schema.object({
            limit: Schema.number(),
            offset: Schema.number(),
            total: Schema.optional(Schema.number())
        });
    }

    /**
     * Common response wrapper
     */
    static response<T extends SchemaNode>(dataSchema: T) {
        return Schema.object({
            success: Schema.boolean(),
            data: dataSchema,
            error: Schema.optional(Schema.string()),
            timestamp: Schema.date()
        });
    }
}