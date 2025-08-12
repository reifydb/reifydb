/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

export interface PrimitiveSchemaNode<T extends string = string> {
    kind: 'primitive';
    type: T;
}

export interface ObjectSchemaNode<P extends Record<string, SchemaNode> = Record<string, SchemaNode>> {
    kind: 'object';
    properties: P;
}

export interface ArraySchemaNode<T extends SchemaNode = SchemaNode> {
    kind: 'array';
    items: T;
}

export interface OptionalSchemaNode<T extends SchemaNode = SchemaNode> {
    kind: 'optional';
    schema: T;
}

export interface ValueSchemaNode<T extends string = string> {
    kind: 'value';
    type: T;
}

export type SchemaNode =
    | PrimitiveSchemaNode
    | ObjectSchemaNode
    | ArraySchemaNode
    | OptionalSchemaNode
    | ValueSchemaNode;

export type {
    PrimitiveToTS,
    PrimitiveToValue,
    InferSchema,
    InferSchemas

} from './inference';

export {
    SchemaBuilder,
    Schema
} from './builder';

export {
    parseValue
} from './parser';

export {
    validateSchema
} from './validator';

