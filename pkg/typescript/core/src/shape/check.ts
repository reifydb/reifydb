// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Column, Frame} from '../types';
import {Type, isOptionType} from '../value';
import {ShapeNode} from '.';
import {widens} from './widen';

export class ShapeMismatch extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ShapeMismatch';
        Object.setPrototypeOf(this, new.target.prototype);
    }
}

export function checkFrames(frames: Frame[], shapes: readonly ShapeNode[]): void {
    if (shapes.length === 0) {
        return;
    }
    if (frames.length !== shapes.length) {
        throw new ShapeMismatch(`expected ${shapes.length} frames for ${shapes.length} shapes, got ${frames.length}`);
    }
    frames.forEach((frame, i) => checkFrame(frame.columns, shapes[i]));
}

export function checkFrame(columns: Column[], shape: ShapeNode): void {
    if (shape.kind !== 'object') {
        throw new ShapeMismatch(`a frame needs an object shape, got ${shape.kind}`);
    }
    for (const [name, propertyShape] of Object.entries(shape.properties)) {
        const column = columns.find(c => c.name === name);
        if (!column) {
            throw new ShapeMismatch(`column "${name}": expected ${describe(propertyShape)}, missing from frame`);
        }
        if (!matches(propertyShape, column.type)) {
            throw new ShapeMismatch(`column "${name}": expected ${describe(propertyShape)}, got ${typeName(column.type)}`);
        }
    }
}

// A primitive shape decodes every wire type whose value shares its representation, so drift
// inside a family (Int4 read as Float8) is not drift; None accepts Option columns because a
// none payload decodes to NoneValue whatever the inner type is.
const FAMILIES: readonly (readonly string[])[] = [
    ['Boolean'],
    ['Int1', 'Int2', 'Int4', 'Int8', 'Int16', 'Uint1', 'Uint2', 'Uint4', 'Uint8', 'Uint16', 'Float4', 'Float8'],
    ['Utf8', 'Decimal', 'Duration', 'Uuid4', 'Uuid7', 'IdentityId'],
    ['Date', 'DateTime', 'Time'],
    ['Blob'],
    ['None'],
];

function familyOf(type: string): readonly string[] {
    return FAMILIES.find(family => family.includes(type)) ?? [type];
}

function matches(shape: ShapeNode, type: Type): boolean {
    switch (shape.kind) {
        case 'primitive':
            if (shape.type === 'None') {
                return type === 'None' || isOptionType(type);
            }
            return !isOptionType(type) && familyOf(shape.type).includes(type);
        case 'value':
            if (shape.type === 'None') {
                return type === 'None' || isOptionType(type);
            }
            return type === shape.type || widens(shape.type, type);
        case 'option':
            return isOptionType(type) && matches(shape.inner, type.Option);
        case 'object':
        case 'array':
            return false;
    }
}

function describe(shape: ShapeNode): string {
    switch (shape.kind) {
        case 'primitive':
        case 'value':
            return shape.type;
        case 'option':
            return `Option(${describe(shape.inner)})`;
        case 'object':
        case 'array':
            return shape.kind;
    }
}

function typeName(type: Type): string {
    return isOptionType(type) ? `Option(${typeName(type.Option)})` : type;
}
