// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {NONE_VALUE} from '../constant';
import {BaseType, Type, isOptionType} from '.';

/**
 * The wire rendering of a type: an object whose `id` names it, with anything it wraps under `underlying`.
 *
 * The client keeps its own representation, where a base type is the bare string it switches on. These two
 * functions are the only crossing between the two, so a wire descriptor never reaches code that expects
 * `{Option: ...}` and a client type never reaches the server expecting `{id, underlying}`.
 */
export interface WireType {
    id: string;
    underlying?: WireType;
}

export function typeToWire(type: Type): WireType {
    return isOptionType(type)
        ? {id: 'Option', underlying: typeToWire(type.Option)}
        : {id: type};
}

export function typeFromWire(wire: WireType): Type {
    if (!wire || typeof wire !== 'object' || typeof wire.id !== 'string') {
        throw new Error(`Expected a type descriptor object, got ${JSON.stringify(wire)}`);
    }
    if (wire.id === 'Option') {
        if (!wire.underlying) {
            throw new Error('Option type descriptor is missing its underlying type');
        }
        return {Option: typeFromWire(wire.underlying)};
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
export function envelopeToColumns(envelope: any): {name: string, type: Type, payload: string[]}[] {
    const types = envelope?.types ?? {};
    const rows: any[] = envelope?.rows ?? [];
    return Object.keys(types).map(name => ({
        name,
        type: typeFromWire(types[name]),
        payload: rows.map(row => payloadOf(row?.[name])),
    }));
}

export function envelopesToFrames(envelopes: any): {columns: {name: string, type: Type, payload: string[]}[]}[] {
    return Array.isArray(envelopes) ? envelopes.map(envelope => ({columns: envelopeToColumns(envelope)})) : [];
}

function payloadOf(value: any): string {
    // A none at the outermost layer is JSON's own null; deeper ones already arrive as a marker.
    if (value === null || value === undefined) {
        return NONE_VALUE;
    }
    return typeof value === 'string' ? value : String(value);
}
