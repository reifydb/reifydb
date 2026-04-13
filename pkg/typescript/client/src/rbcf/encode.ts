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
import { encode_bitvec } from "./nones";
import type { WireColumn, WireFrame } from "./types";

export function encode(frames: WireFrame[]): Uint8Array {
    const w = new BinaryWriter(4096);
    const msg_header_at = w.reserve(MESSAGE_HEADER_SIZE);

    for (const frame of frames) encode_frame(w, frame);

    const total_size = w.length;
    // Patch message header at the very end so the writer's internal buffer is stable.
    w.patch_u32(msg_header_at + 0, RBCF_MAGIC);
    w.patch_u16(msg_header_at + 4, RBCF_VERSION);
    w.patch_u16(msg_header_at + 6, 0); // flags
    w.patch_u32(msg_header_at + 8, frames.length);
    w.patch_u32(msg_header_at + 12, total_size);

    return w.finish().slice();
}

function encode_frame(w: BinaryWriter, frame: WireFrame): void {
    const frame_start = w.length;
    const row_count = frame.columns[0]?.payload.length ?? 0;
    const col_count = frame.columns.length;

    let meta_flags = 0;
    if (frame.row_numbers && frame.row_numbers.length > 0) meta_flags |= META_HAS_ROW_NUMBERS;
    if (frame.created_at && frame.created_at.length > 0) meta_flags |= META_HAS_CREATED_AT;
    if (frame.updated_at && frame.updated_at.length > 0) meta_flags |= META_HAS_UPDATED_AT;

    const frame_header_at = w.reserve(FRAME_HEADER_SIZE);

    if (meta_flags & META_HAS_ROW_NUMBERS) {
        for (const rn of frame.row_numbers!) w.u64(BigInt(rn));
    }
    if (meta_flags & (META_HAS_CREATED_AT | META_HAS_UPDATED_AT)) {
        // Encoding created_at/updated_at requires u64 nanos-since-epoch. WireFrame carries
        // ISO strings (the JSON form); round-tripping would lose sub-microsecond precision.
        // Server does not expect client-encoded frames, so we reject rather than truncate.
        throw new Error("RBCF encode: metadata timestamps require nanos input, got ISO strings");
    }

    for (const col of frame.columns) encode_column(w, col);

    const frame_size = w.length - frame_start;
    w.patch_u32(frame_header_at + 0, row_count);
    w.patch_u16(frame_header_at + 4, col_count);
    w.patch_u8(frame_header_at + 6, meta_flags);
    w.patch_u8(frame_header_at + 7, 0);
    w.patch_u32(frame_header_at + 8, frame_size);
}

function type_info(t: Type): { base: TypeName; is_option: boolean } {
    if (typeof t === "object" && t !== null && "Option" in t) {
        const inner = type_info((t as { Option: Type }).Option);
        return { base: inner.base, is_option: true };
    }
    return { base: t as TypeName, is_option: false };
}

function encode_column(w: BinaryWriter, col: WireColumn): void {
    const { base, is_option } = type_info(col.type);
    const row_count = col.payload.length;

    const type_code = TYPE_CODE[base] | (is_option ? TYPE_OPTION_FLAG : 0);

    const defined = new Array<boolean>(row_count);
    const defined_payload = new Array<string>(row_count);
    for (let i = 0; i < row_count; i++) {
        const cell = col.payload[i];
        if (cell === NONE_VALUE) {
            defined[i] = false;
            defined_payload[i] = placeholder_for(base);
        } else {
            defined[i] = true;
            defined_payload[i] = cell;
        }
    }

    const has_nones = is_option && defined.some((d) => !d);
    const nones_bytes = has_nones ? encode_bitvec(defined) : new Uint8Array(0);

    const { data, offsets } = encode_plain_data(base, defined_payload);

    // Column descriptor (28 bytes).
    w.u8(type_code);
    w.u8(ColumnEncoding.Plain);
    w.u8(has_nones ? COL_FLAG_HAS_NONES : 0);
    w.u8(0); // reserved
    const name_bytes = new TextEncoder().encode(col.name);
    w.u16(name_bytes.length);
    w.u16(0); // reserved2
    w.u32(row_count);
    w.u32(nones_bytes.length);
    w.u32(data.length);
    w.u32(offsets.length);
    w.u32(0); // extra_len — Plain has no extra segment.
    void COLUMN_DESCRIPTOR_SIZE;

    // Name + 4-byte padding.
    w.bytes(name_bytes);
    const pad = (4 - (name_bytes.length % 4)) % 4;
    if (pad > 0) w.zeroes(pad);

    w.bytes(nones_bytes);
    w.bytes(data);
    w.bytes(offsets);
}

function placeholder_for(base: TypeName): string {
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

function encode_plain_data(base: TypeName, cells: string[]): EncodedData {
    const w = new BinaryWriter(cells.length * 8);
    switch (base) {
        case "Boolean": {
            const bits = cells.map((s) => s === "true");
            return { data: encode_bitvec(bits), offsets: new Uint8Array(0) };
        }
        case "Int1":
            for (const s of cells) w.i8(parse_int_strict(s));
            return empty(w);
        case "Int2":
            for (const s of cells) w.i16(parse_int_strict(s));
            return empty(w);
        case "Int4":
            for (const s of cells) w.i32(parse_int_strict(s));
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
            for (const s of cells) w.f32(parse_float_strict(s));
            return empty(w);
        case "Float8":
            for (const s of cells) w.f64(parse_float_strict(s));
            return empty(w);
        case "Utf8": {
            const bytes_list = cells.map((s) => new TextEncoder().encode(s));
            return encode_varlen(bytes_list);
        }
        case "Blob": {
            const bytes_list = cells.map((s) => hex_string_to_bytes(s));
            return encode_varlen(bytes_list);
        }
        default:
            throw new Error(`RBCF encode: type ${base} not supported by the pure-TS encoder (Plain-only)`);
    }
}

function empty(w: BinaryWriter): EncodedData {
    return { data: w.finish().slice(), offsets: new Uint8Array(0) };
}

function encode_varlen(entries: Uint8Array[]): EncodedData {
    const offsets_w = new BinaryWriter((entries.length + 1) * 4);
    const data_w = new BinaryWriter();
    let offset = 0;
    offsets_w.u32(0);
    for (const e of entries) {
        data_w.bytes(e);
        offset += e.length;
        offsets_w.u32(offset);
    }
    return { data: data_w.finish().slice(), offsets: offsets_w.finish().slice() };
}

function parse_int_strict(s: string): number {
    const n = Number(s);
    if (!Number.isFinite(n) || !Number.isInteger(n)) {
        throw new Error(`RBCF encode: invalid integer '${s}'`);
    }
    return n | 0;
}

function parse_float_strict(s: string): number {
    if (s === "NaN") return NaN;
    if (s === "inf") return Infinity;
    if (s === "-inf") return -Infinity;
    return Number(s);
}

function hex_string_to_bytes(s: string): Uint8Array {
    const hex = s.startsWith("0x") ? s.slice(2) : s;
    if (hex.length % 2 !== 0) throw new Error(`RBCF encode: invalid hex length in blob`);
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}
