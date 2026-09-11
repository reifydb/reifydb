// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Decoding and dispatch for subscription change traffic, kept clear of any one transport.
// The server emits the same encoded envelopes over the WebSocket and over the native bridge,
// so the turn from wire bytes to callback rows lives here once rather than once per socket.

import {
    decode,
    framesFromWire,
    transformResult,
    checkFrame,
    ROW_NUMBER_KEY
} from "@reifydb/core";
import type {
    ShapeNode,
} from "@reifydb/core";

import type {
    ResponseMeta,
    SubscriptionCallbacks
} from "./types";
import {CONTENT_TYPE_FRAMES} from "./content-types";

export enum BinaryKind {
    Response = 0x00,
    Change = 0x01,
    BatchChange = 0x02,
}

export interface BinaryEnvelope {
    kind: BinaryKind;
    id: string;
    meta?: ResponseMeta;
    rbcf: Uint8Array;
}

// A transport keeps whatever bookkeeping its own registry needs; dispatch only ever reaches for
// the callbacks and the shape, so each transport hands its own subscription record over as-is.
export interface SubscriptionTarget {
    callbacks: SubscriptionCallbacks<any>;
    shape?: ShapeNode;
}

export function decodeEnvelope(bytes: Uint8Array): BinaryEnvelope | null {
    if (bytes.length < 5) return null;
    const kind = bytes[0] as BinaryKind;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const idLen = view.getUint32(1, true);
    if (bytes.length < 5 + idLen + 4) return null;
    const decoder = new TextDecoder("utf-8");
    const id = decoder.decode(bytes.subarray(5, 5 + idLen));

    const metaLen = view.getUint32(5 + idLen, true);
    if (bytes.length < 5 + idLen + 4 + metaLen) return null;

    let meta: ResponseMeta | undefined;
    if (metaLen > 0) {
        const metaJson = decoder.decode(bytes.subarray(5 + idLen + 4, 5 + idLen + 4 + metaLen));
        try {
            meta = JSON.parse(metaJson);
        } catch (e) {
            console.error("Failed to parse RBCF metadata", e);
        }
    }

    const rbcfBytes = bytes.subarray(5 + idLen + 4 + metaLen);
    return {kind, id, meta, rbcf: rbcfBytes};
}

export interface BatchBinaryEnvelope {
    batchId: string;
    entries: Array<{ subscriptionId: string; rbcf: Uint8Array }>;
}

export function decodeBatchEnvelope(bytes: Uint8Array): BatchBinaryEnvelope | null {
    if (bytes.length < 9) return null;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const decoder = new TextDecoder("utf-8");

    const batchIdLen = view.getUint32(1, true);
    let offset = 5;
    if (bytes.length < offset + batchIdLen + 4) return null;
    const batchId = decoder.decode(bytes.subarray(offset, offset + batchIdLen));
    offset += batchIdLen;

    const numEntries = view.getUint32(offset, true);
    offset += 4;

    const entries: Array<{ subscriptionId: string; rbcf: Uint8Array }> = [];
    for (let i = 0; i < numEntries; i++) {
        if (bytes.length < offset + 4) return null;
        const subIdLen = view.getUint32(offset, true);
        offset += 4;
        if (bytes.length < offset + subIdLen + 4) return null;
        const subscriptionId = decoder.decode(bytes.subarray(offset, offset + subIdLen));
        offset += subIdLen;

        const rbcfLen = view.getUint32(offset, true);
        offset += 4;
        if (bytes.length < offset + rbcfLen) return null;
        const rbcfBytes = bytes.subarray(offset, offset + rbcfLen);
        offset += rbcfLen;

        entries.push({subscriptionId, rbcf: rbcfBytes});
    }

    return {batchId, entries};
}

export function dispatchChange(target: SubscriptionTarget, contentType: string, body: any): void {
    // The frames content type carries each column's type in the wire's rendering; rbcf changes were
    // already decoded into the client's own types on the way in.
    const raw = body?.frames || [];
    const fromWire = contentType === CONTENT_TYPE_FRAMES;
    for (const rawFrame of raw) {
        // One undecodable frame must not abandon the frames after it, nor escape into the
        // socket handler, where it would stall every later message on this connection. The
        // wire types are read per frame for that reason: a malformed descriptor is the one
        // frame's problem, not the whole message's.
        try {
            dispatchChangeFrame(target, fromWire ? framesFromWire([rawFrame])[0] : rawFrame);
        } catch (error) {
            reportSubscriptionError(target, error);
        }
    }
}

export function reportSubscriptionError(target: SubscriptionTarget, error: unknown): void {
    const reported = error instanceof Error ? error : new Error(String(error));
    if (target.callbacks.onError) {
        target.callbacks.onError(reported);
        return;
    }
    console.error('Subscription change could not be delivered:', reported);
}

export function dispatchChangeFrame(target: SubscriptionTarget, frame: any): void {
    const rows = frameToRows(frame, target.shape);
    if (rows.length === 0) return;

    switch (frame.op) {
        case 2:
            target.callbacks.onUpdate?.(rows);
            break;
        case 3:
            target.callbacks.onRemove?.(rows);
            break;
        default:
            target.callbacks.onInsert?.(rows);
            break;
    }
}

export function frameToRows(frame: any, shape?: ShapeNode): any[] {
    if (!frame.columns || frame.columns.length === 0) return [];
    if (shape) checkFrame(frame.columns, shape);

    const rowCount = frame.columns[0].payload.length;
    const rowNumbers = frame.row_numbers;
    const rows: any[] = [];

    for (let i = 0; i < rowCount; i++) {
        const row: any = {};
        for (const col of frame.columns) {
            row[col.name] = decode({type: col.type, value: col.payload[i]});
        }
        rows.push(row);
    }

    const shaped = shape ? rows.map(row => transformResult(row, shape)) : rows;

    if (rowNumbers) {
        for (let i = 0; i < shaped.length; i++) {
            if (rowNumbers[i] !== undefined) shaped[i][ROW_NUMBER_KEY] = Number(rowNumbers[i]);
        }
    }

    return shaped;
}
