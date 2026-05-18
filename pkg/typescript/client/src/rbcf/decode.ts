// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// RBCF message decoder: Uint8Array -> WireFrame[].
// Port of crates/wire-format/src/decode/mod.rs.

import type { Type } from "@reifydb/core";
import { NONE_VALUE } from "@reifydb/core";

import {
    COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, ColumnEncoding, FRAME_HEADER_SIZE,
    META_HAS_CREATED_AT, META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT, MESSAGE_HEADER_SIZE,
    RBCF_MAGIC, RBCF_VERSION, dict_index_width_from_flags, is_option_code, type_name_from_code,
} from "./format";
import { BinaryReader } from "./reader";
import { decode_bitvec } from "./nones";
import { format_date_time } from "./values";
import type { WireColumn, WireFrame } from "./types";
import { decode_plain } from "./encoding/plain";
import { decode_dict } from "./encoding/dict";
import { decode_rle } from "./encoding/rle";
import { decode_delta, decode_delta_rle } from "./encoding/delta";

export function decode(bytes: Uint8Array): WireFrame[] {
    const r = new BinaryReader(bytes);

    const magic = r.u32();
    if (magic !== RBCF_MAGIC) {
        throw new Error(`RBCF: invalid magic 0x${magic.toString(16)} (expected 0x${RBCF_MAGIC.toString(16)})`);
    }
    const version = r.u16();
    if (version !== RBCF_VERSION) {
        throw new Error(`RBCF: unsupported version ${version} (expected ${RBCF_VERSION})`);
    }
    r.u16(); // flags (reserved)
    const frame_count = r.u32();
    r.u32(); // total_size (trusted; we stop at EOF if malformed)

    const frames: WireFrame[] = [];
    for (let i = 0; i < frame_count; i++) frames.push(decode_frame(r));
    return frames;
}

function decode_frame(r: BinaryReader): WireFrame {
    const frame_start = r.pos;
    if (r.remaining() < FRAME_HEADER_SIZE) throw new Error("RBCF: frame header truncated");

    const row_count = r.u32();
    const col_count = r.u16();
    const meta_flags = r.u8();
    r.u8(); // reserved
    r.u32(); // frame_size
    void frame_start;

    const frame: WireFrame = { columns: [] };

    if (meta_flags & META_HAS_ROW_NUMBERS) {
        const rows = new Array<string>(row_count);
        for (let i = 0; i < row_count; i++) rows[i] = r.u64().toString();
        frame.row_numbers = rows;
    }
    if (meta_flags & META_HAS_CREATED_AT) {
        const cr = new Array<string>(row_count);
        for (let i = 0; i < row_count; i++) cr[i] = format_date_time(r.u64());
        frame.created_at = cr;
    }
    if (meta_flags & META_HAS_UPDATED_AT) {
        const up = new Array<string>(row_count);
        for (let i = 0; i < row_count; i++) up[i] = format_date_time(r.u64());
        frame.updated_at = up;
    }

    for (let c = 0; c < col_count; c++) frame.columns.push(decode_column(r));
    return frame;
}

function decode_column(r: BinaryReader): WireColumn {
    if (r.remaining() < COLUMN_DESCRIPTOR_SIZE) throw new Error("RBCF: column descriptor truncated");

    const type_code = r.u8();
    const encoding_byte = r.u8();
    const flags = r.u8();
    r.u8(); // reserved
    const name_len = r.u16();
    r.u16(); // reserved2
    const row_count = r.u32();
    const nones_len = r.u32();
    const data_len = r.u32();
    const offsets_len = r.u32();
    const extra_len = r.u32();

    const encoding = encoding_byte as ColumnEncoding;
    const has_nones = (flags & COL_FLAG_HAS_NONES) !== 0;
    const option_outer = is_option_code(type_code);

    const name = r.utf8(name_len);
    const name_pad = (4 - (name_len % 4)) % 4;
    if (name_pad > 0) r.skip(name_pad);

    // Slice each segment.
    const nones_bytes = r.bytes(nones_len);
    const data_bytes = r.bytes(data_len);
    const offsets_bytes = r.bytes(offsets_len);
    const extra_bytes = r.bytes(extra_len);

    const base_name = type_name_from_code(type_code);
    let payload: string[];

    try {
        payload = decode_by_strategy(base_name, encoding, flags, row_count, data_bytes, offsets_bytes, extra_bytes);
    } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        throw new Error(`RBCF: column '${name}' decode failed: ${msg}`);
    }

    // Overlay nones bitmap: absent cells become NONE_VALUE sentinel (matches JSON path).
    if (has_nones && nones_len > 0) {
        const bits = decode_bitvec(nones_bytes, row_count);
        for (let i = 0; i < row_count; i++) {
            if (!bits[i]) payload[i] = NONE_VALUE;
        }
    }

    const final_type: Type = option_outer ? { Option: base_name as Type } : (base_name as Type);
    return { name, type: final_type, payload };
}

function decode_by_strategy(
    type_name: ReturnType<typeof type_name_from_code>,
    encoding: ColumnEncoding,
    flags: number,
    row_count: number,
    data: Uint8Array,
    offsets: Uint8Array,
    extra: Uint8Array
): string[] {
    switch (encoding) {
        case ColumnEncoding.Plain:
        case ColumnEncoding.BitPack:
            return decode_plain(type_name, row_count, data, offsets);
        case ColumnEncoding.Dict:
            return decode_dict(type_name, row_count, data, extra, dict_index_width_from_flags(flags));
        case ColumnEncoding.Rle:
            return decode_rle(type_name, row_count, data);
        case ColumnEncoding.Delta:
            return decode_delta(type_name, row_count, data);
        case ColumnEncoding.DeltaRle:
            return decode_delta_rle(type_name, row_count, data);
        default:
            throw new Error(`RBCF: unknown encoding ${encoding}`);
    }
}
