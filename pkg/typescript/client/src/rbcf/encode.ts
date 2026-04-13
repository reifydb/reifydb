// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// RBCF message encoder: WireFrame[] -> Uint8Array.
// Implements Plain encoding only (no Dict/RLE/Delta heuristics). The server controls
// compression; the client path only needs a spec-compliant encoder for params and for
// any caller that wants to round-trip.

import { NONE_VALUE } from "@reifydb/core";
import type { Type } from "@reifydb/core";

import {
    COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, ColumnEncoding, FRAME_HEADER_SIZE,
    MESSAGE_HEADER_SIZE, META_HAS_CREATED_AT, META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT,
    RBCF_MAGIC, RBCF_VERSION, TYPE_CODE, TYPE_OPTION_FLAG, type TypeName,
} from "./format";
import { BinaryWriter } from "./writer";
import { encodeBitvec } from "./nones";
import type { WireColumn, WireFrame } from "./types";

export function encode(frames: WireFrame[]): Uint8Array {
    const w = new BinaryWriter(4096);
    const msgHeaderAt = w.reserve(MESSAGE_HEADER_SIZE);

    for (const frame of frames) encodeFrame(w, frame);

    const totalSize = w.length;
    // Patch message header at the very end so the writer's internal buffer is stable.
    w.patchU32(msgHeaderAt + 0, RBCF_MAGIC);
    w.patchU16(msgHeaderAt + 4, RBCF_VERSION);
    w.patchU16(msgHeaderAt + 6, 0); // flags
    w.patchU32(msgHeaderAt + 8, frames.length);
    w.patchU32(msgHeaderAt + 12, totalSize);

    return w.finish().slice();
}

function encodeFrame(w: BinaryWriter, frame: WireFrame): void {
    const frameStart = w.length;
    const rowCount = frame.columns[0]?.payload.length ?? 0;
    const colCount = frame.columns.length;

    let metaFlags = 0;
    if (frame.row_numbers && frame.row_numbers.length > 0) metaFlags |= META_HAS_ROW_NUMBERS;
    if (frame.created_at && frame.created_at.length > 0) metaFlags |= META_HAS_CREATED_AT;
    if (frame.updated_at && frame.updated_at.length > 0) metaFlags |= META_HAS_UPDATED_AT;

    const frameHeaderAt = w.reserve(FRAME_HEADER_SIZE);

    if (metaFlags & META_HAS_ROW_NUMBERS) {
        for (const rn of frame.row_numbers!) w.u64(BigInt(rn));
    }
    if (metaFlags & (META_HAS_CREATED_AT | META_HAS_UPDATED_AT)) {
        // Encoding created_at/updated_at requires u64 nanos-since-epoch. WireFrame carries
        // ISO strings (the JSON form); round-tripping would lose sub-microsecond precision.
        // Server does not expect client-encoded frames, so we reject rather than truncate.
        throw new Error("RBCF encode: metadata timestamps require nanos input, got ISO strings");
    }

    for (const col of frame.columns) encodeColumn(w, col);

    const frameSize = w.length - frameStart;
    w.patchU32(frameHeaderAt + 0, rowCount);
    w.patchU16(frameHeaderAt + 4, colCount);
    w.patchU8(frameHeaderAt + 6, metaFlags);
    w.patchU8(frameHeaderAt + 7, 0);
    w.patchU32(frameHeaderAt + 8, frameSize);
}

function typeInfo(t: Type): { base: TypeName; isOption: boolean } {
    if (typeof t === "object" && t !== null && "Option" in t) {
        const inner = typeInfo((t as { Option: Type }).Option);
        return { base: inner.base, isOption: true };
    }
    return { base: t as TypeName, isOption: false };
}

function encodeColumn(w: BinaryWriter, col: WireColumn): void {
    const { base, isOption } = typeInfo(col.type);
    const rowCount = col.payload.length;

    const typeCode = TYPE_CODE[base] | (isOption ? TYPE_OPTION_FLAG : 0);

    const defined = new Array<boolean>(rowCount);
    const definedPayload = new Array<string>(rowCount);
    for (let i = 0; i < rowCount; i++) {
        const cell = col.payload[i];
        if (cell === NONE_VALUE) {
            defined[i] = false;
            definedPayload[i] = placeholderFor(base);
        } else {
            defined[i] = true;
            definedPayload[i] = cell;
        }
    }

    const hasNones = isOption && defined.some((d) => !d);
    const nonesBytes = hasNones ? encodeBitvec(defined) : new Uint8Array(0);

    const { data, offsets } = encodePlainData(base, definedPayload);

    // Column descriptor (28 bytes).
    w.u8(typeCode);
    w.u8(ColumnEncoding.Plain);
    w.u8(hasNones ? COL_FLAG_HAS_NONES : 0);
    w.u8(0); // reserved
    const nameBytes = new TextEncoder().encode(col.name);
    w.u16(nameBytes.length);
    w.u16(0); // reserved2
    w.u32(rowCount);
    w.u32(nonesBytes.length);
    w.u32(data.length);
    w.u32(offsets.length);
    w.u32(0); // extra_len — Plain has no extra segment.
    void COLUMN_DESCRIPTOR_SIZE;

    // Name + 4-byte padding.
    w.bytes(nameBytes);
    const pad = (4 - (nameBytes.length % 4)) % 4;
    if (pad > 0) w.zeroes(pad);

    w.bytes(nonesBytes);
    w.bytes(data);
    w.bytes(offsets);
}

function placeholderFor(base: TypeName): string {
    switch (base) {
        case "Boolean": return "false";
        case "Utf8":
        case "Blob":
        case "Decimal":
        case "Int":
        case "Uint":
        case "Any": return "";
        case "Float4":
        case "Float8": return "0";
        case "Date": return "1970-01-01";
        case "DateTime": return "1970-01-01T00:00:00.000000000Z";
        case "Time": return "00:00:00.000000000";
        case "Duration": return "0s";
        case "IdentityId":
        case "Uuid4":
        case "Uuid7": return "00000000-0000-0000-0000-000000000000";
        case "DictionaryId": return "0";
        default: return "0";
    }
}

interface EncodedData { data: Uint8Array; offsets: Uint8Array; }

function encodePlainData(base: TypeName, cells: string[]): EncodedData {
    const w = new BinaryWriter(cells.length * 8);
    switch (base) {
        case "Boolean": {
            const bits = cells.map((s) => s === "true");
            return { data: encodeBitvec(bits), offsets: new Uint8Array(0) };
        }
        case "Int1":
            for (const s of cells) w.i8(parseIntStrict(s));
            return empty(w);
        case "Int2":
            for (const s of cells) w.i16(parseIntStrict(s));
            return empty(w);
        case "Int4":
            for (const s of cells) w.i32(parseIntStrict(s));
            return empty(w);
        case "Int8":
            for (const s of cells) w.i64(BigInt(s));
            return empty(w);
        case "Int16":
            for (const s of cells) w.i128(BigInt(s));
            return empty(w);
        case "Uint1":
            for (const s of cells) w.u8(Number(s));
            return empty(w);
        case "Uint2":
            for (const s of cells) w.u16(Number(s));
            return empty(w);
        case "Uint4":
            for (const s of cells) w.u32(Number(s));
            return empty(w);
        case "Uint8":
            for (const s of cells) w.u64(BigInt(s));
            return empty(w);
        case "Uint16":
            for (const s of cells) w.u128(BigInt(s));
            return empty(w);
        case "Float4":
            for (const s of cells) w.f32(parseFloatStrict(s));
            return empty(w);
        case "Float8":
            for (const s of cells) w.f64(parseFloatStrict(s));
            return empty(w);
        case "Utf8": {
            const bytesList = cells.map((s) => new TextEncoder().encode(s));
            return encodeVarlen(bytesList);
        }
        case "Blob": {
            const bytesList = cells.map((s) => hexStringToBytes(s));
            return encodeVarlen(bytesList);
        }
        default:
            throw new Error(`RBCF encode: type ${base} not supported by the pure-TS encoder (Plain-only)`);
    }
}

function empty(w: BinaryWriter): EncodedData {
    return { data: w.finish().slice(), offsets: new Uint8Array(0) };
}

function encodeVarlen(entries: Uint8Array[]): EncodedData {
    const offsetsW = new BinaryWriter((entries.length + 1) * 4);
    const dataW = new BinaryWriter();
    let offset = 0;
    offsetsW.u32(0);
    for (const e of entries) {
        dataW.bytes(e);
        offset += e.length;
        offsetsW.u32(offset);
    }
    return { data: dataW.finish().slice(), offsets: offsetsW.finish().slice() };
}

function parseIntStrict(s: string): number {
    const n = Number(s);
    if (!Number.isFinite(n) || !Number.isInteger(n)) {
        throw new Error(`RBCF encode: invalid integer '${s}'`);
    }
    return n | 0;
}

function parseFloatStrict(s: string): number {
    if (s === "NaN") return NaN;
    if (s === "inf") return Infinity;
    if (s === "-inf") return -Infinity;
    return Number(s);
}

function hexStringToBytes(s: string): Uint8Array {
    const hex = s.startsWith("0x") ? s.slice(2) : s;
    if (hex.length % 2 !== 0) throw new Error(`RBCF encode: invalid hex length in blob`);
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}
