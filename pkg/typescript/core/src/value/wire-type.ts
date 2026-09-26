// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {NONE_VALUE, ROW_NUMBER_KEY} from '../constant';
import {BaseType, Type, WireCellValue, isDigestType, isListType, isOptionType, isRecordType} from '.';
import {digestType} from './digest';
import {
    DECIMAL_DEFAULT_SCALE, FIXED_POINT_MAX_PRECISION, decimalType, fixedPointKind, fixedPointPrecision,
    fixedPointScale, isFixedPointType,
} from './fixed-point';

function peelOption(type: Type): Type {
    return isOptionType(type) ? peelOption(type.Option) : type;
}

/**
 * The wire rendering of a type: an object whose `id` names it, with anything it wraps under `underlying`.
 *
 * The client keeps its own representation, where a base type is the bare string it switches on. These two
 * functions are the only crossing between the two, so a wire descriptor never reaches code that expects
 * `{Option: ...}` and a client type never reaches the server expecting `{id, underlying}`.
 */
export interface WireRecordField {
    name: string;
    type: WireType;
}

export interface WireType {
    id: string;
    underlying?: WireType | WireRecordField[];
    accuracy?: number;
    precision?: number;
    scale?: number;
}

export function typeToWire(type: Type): WireType {
    if (isOptionType(type)) {
        return {id: 'Option', underlying: typeToWire(type.Option)};
    }
    if (isDigestType(type)) {
        return {id: 'Digest', underlying: {id: type.Digest.inner}, accuracy: type.Digest.accuracy};
    }
    if (isFixedPointType(type)) {
        const kind = fixedPointKind(type);
        const wire: WireType = {id: kind, precision: fixedPointPrecision(type)};
        if (kind === 'Decimal') wire.scale = fixedPointScale(type);
        return wire;
    }
    if (isListType(type)) {
        return {id: 'List', underlying: typeToWire(type.List)};
    }
    if (isRecordType(type)) {
        return {id: 'Record', underlying: type.Record.map(field => ({name: field.name, type: typeToWire(field.type)}))};
    }
    return {id: type};
}

export function typeFromWire(wire: WireType): Type {
    if (!wire || typeof wire !== 'object' || typeof wire.id !== 'string') {
        throw new Error(`Expected a type descriptor object, got ${JSON.stringify(wire)}`);
    }
    if (wire.id === 'Option') {
        if (!wire.underlying || Array.isArray(wire.underlying)) {
            throw new Error('Option type descriptor is missing its underlying type');
        }
        return {Option: typeFromWire(wire.underlying)};
    }
    if (wire.id === 'Digest') {
        const inner = !Array.isArray(wire.underlying) ? wire.underlying?.id : undefined;
        if (typeof inner !== 'string' || typeof wire.accuracy !== 'number') {
            throw new Error(`Digest type descriptor needs an underlying type and a numeric accuracy, got ${JSON.stringify(wire)}`);
        }
        return digestType(inner, wire.accuracy);
    }
    if (wire.id === 'Decimal') {
        const precision = wire.precision ?? FIXED_POINT_MAX_PRECISION;
        return decimalType(precision, wire.scale ?? DECIMAL_DEFAULT_SCALE);
    }
    if (wire.id === 'List') {
        if (!wire.underlying || Array.isArray(wire.underlying)) {
            throw new Error('List type descriptor is missing its underlying type');
        }
        return {List: typeFromWire(wire.underlying)};
    }
    if (wire.id === 'Record') {
        if (!Array.isArray(wire.underlying)) {
            throw new Error('Record type descriptor needs an underlying array of fields');
        }
        return {Record: wire.underlying.map(field => ({name: field.name, type: typeFromWire(field.type)}))};
    }
    return wire.id as BaseType;
}

/** Reads the column types of a frames payload, leaving the rest of the frame untouched. */
export function columnsFromWire<T extends {type: any}>(columns: T[]): T[] {
    return columns.map(column => ({...column, type: typeFromWire(column.type)}));
}

export function framesFromWire(frames: any[]): any[] {
    return frames.map(frame =>
        frame && frame.columns ? {...frame, columns: columnsFromWire(frame.columns)} : frame
    );
}

/**
 * Reads a `format=json` frame envelope into the column shape the decoder works on.
 *
 * The rows carry one value per column and the envelope's `types` says what each column is, so the two
 * together hold everything the frames format carries. Turning them back into columns lets the JSON
 * transports run the same decode and the same shape check as every other transport, instead of handing
 * the caller whatever JSON happened to arrive.
 */
export function envelopeToColumns(envelope: any): {name: string, type: Type, payload: WireCellValue[]}[] {
    const types = envelope?.types;
    const rows: any[] = envelope?.rows ?? [];
    if (!types || typeof types !== 'object') {
        if (rows.length > 0) {
            throw new Error(`Frame envelope carries rows but no column types, got ${JSON.stringify(envelope)}`);
        }
        return [];
    }
    return Object.keys(types).map(name => {
        const type = typeFromWire(types[name]);
        return {
            name,
            type,
            payload: rows.map(row => payloadOf(row, name, type)),
        };
    });
}

export function envelopesToFrames(envelopes: any): {columns: {name: string, type: Type, payload: WireCellValue[]}[], row_numbers?: number[]}[] {
    if (!Array.isArray(envelopes)) {
        throw new Error(`Expected a list of frame envelopes, got ${JSON.stringify(envelopes)}`);
    }
    return envelopes.map(envelope => {
        const rows: any[] = envelope?.rows ?? [];
        return {
            columns: envelopeToColumns(envelope),
            row_numbers: rows.length > 0 && ROW_NUMBER_KEY in rows[0] ? rows.map(row => row[ROW_NUMBER_KEY]) : undefined,
        };
    });
}

function payloadOf(row: any, name: string, type: Type): WireCellValue {
    if (!row || typeof row !== 'object' || !(name in row)) {
        throw new Error(`Row is missing a cell for column ${name}`);
    }
    const value = row[name];
    if (value === null || value === undefined) {
        if (!isOptionType(type)) {
            throw new Error(`A none cell cannot fit the non-option column ${name}`);
        }
        return NONE_VALUE;
    }
    const base = peelOption(type);
    if (isListType(base) || isRecordType(base)) {
        return value as WireCellValue;
    }
    if (typeof value !== 'string') {
        throw new Error(`Cell for column ${name} must arrive as text, got ${typeof value}`);
    }
    return value;
}
