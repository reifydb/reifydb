// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {checkFrames, columnsToRows, envelopesToFrames, transformFrames} from "@reifydb/core";
import type {ShapeNode} from "@reifydb/core";

/**
 * A `format=json` response is one envelope per frame, each carrying its rows and the type of every column.
 * A raw body and an unwrapped response carry neither, so they are recognised here and passed through.
 */
function isEnvelopeList(data: any): boolean {
    return Array.isArray(data)
        && data.every(entry => entry !== null && typeof entry === 'object' && !Array.isArray(entry)
            && Array.isArray(entry.rows));
}

/**
 * Decodes a `format=json` response.
 *
 * The wire writes every number as text so no value is rounded to a double or reformatted by a runtime on
 * the way, which means the rows alone cannot say what they hold. The envelope's types say it, so the rows
 * are decoded here into values that carry their own type, and a shape is checked exactly as the frames and
 * rbcf transports check it. Before the response carried types, a shape here only renamed keys: an Int4
 * stayed the string the wire sent, a value shape was never constructed, and a shape that did not describe
 * the data resolved instead of raising.
 */
export function decodeJsonResponse(data: any, shapes?: readonly ShapeNode[]): any {
    if (!isEnvelopeList(data)) {
        return transformFrames(data ?? [], shapes ?? []);
    }

    const frames = envelopesToFrames(data);
    if (shapes && shapes.length > 0) {
        checkFrames(frames, shapes);
    }
    return transformFrames(frames.map(frame => columnsToRows(frame.columns)), shapes ?? []);
}

/**
 * An unwrapped response is the bare value the caller asked for, with no envelope to carry types, so it
 * keeps the shape transform it has always had.
 */
export function decodeUnwrappedJsonResponse(data: any, shapes?: readonly ShapeNode[]): any {
    return transformFrames(data ?? [], shapes ?? []);
}

/**
 * Decodes a `format=json` response into rows and leaves the shape transform to the caller.
 *
 * The root clients read every wire format through one send and apply the transform themselves
 * afterwards, so they need the rows rather than the finished result. A body that is not an envelope
 * list is passed through: an unwrapped response has no types to read.
 */
export function jsonResponseToRows(data: any, shapes?: readonly ShapeNode[]): any[] {
    if (!isEnvelopeList(data)) {
        return data ?? [];
    }

    const frames = envelopesToFrames(data);
    if (shapes && shapes.length > 0) {
        checkFrames(frames, shapes);
    }
    return frames.map(frame => columnsToRows(frame.columns));
}
