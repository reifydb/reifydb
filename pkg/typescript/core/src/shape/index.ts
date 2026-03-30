// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
export interface PrimitiveShapeNode<T extends string = string> {
    kind: 'primitive';
    type: T;
}

export interface ObjectShapeNode<P extends Record<string, ShapeNode> = Record<string, ShapeNode>> {
    kind: 'object';
    properties: P;
}

export interface ArrayShapeNode<T extends ShapeNode = ShapeNode> {
    kind: 'array';
    items: T;
}

export interface OptionalShapeNode<T extends ShapeNode = ShapeNode> {
    kind: 'optional';
    shape: T;
}

export interface ValueShapeNode<T extends string = string> {
    kind: 'value';
    type: T;
}

export type ShapeNode =
    | PrimitiveShapeNode
    | ObjectShapeNode
    | ArrayShapeNode
    | OptionalShapeNode
    | ValueShapeNode;

export type {
    PrimitiveToTS,
    PrimitiveToValue,
    InferShape,
    InferShapes

} from './inference';

export {
    ShapeBuilder,
    Shape
} from './builder';

export {
    parseValue
} from './parser';

export {
    validateShape
} from './validator';
