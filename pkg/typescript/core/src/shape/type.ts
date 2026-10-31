// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {ShapeNode} from '.';
import {ShapeMismatch} from './check';
import {Type} from '../value';

export function shapeType(shape: ShapeNode): Type {
    switch (shape.kind) {
        case 'primitive':
        case 'value':
            return shape.type as Type;
        case 'option':
            return {Option: shapeType(shape.inner)};
        case 'object':
        case 'array':
            throw new ShapeMismatch(`a ${shape.kind} shape has no wire type`);
    }
}
