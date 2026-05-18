// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Run-length encoding. Port of crates/wire-format/src/encoding/rle.rs and
// the dispatch in crates/wire-format/src/decode/fixed.rs decode_rle_column.

import { type TypeName } from "../format";
import {
    read_f32, read_f64, read_i16, read_i32, read_i64, read_i128,
    read_u16, read_u32, read_u64, read_u128,
} from "../reader";
import {
    format_date, format_date_time, format_f32, format_f64, format_time, signed_big_int_from_le_bytes,
} from "../values";

// Fixed-size RLE: (value, u32 run_length) pairs.
function decode_rle_fixed(
    data: Uint8Array,
    row_count: number,
    elem_size: number,
    decode_value: (buf: Uint8Array, pos: number) => string
): string[] {
    const out: string[] = [];
    let pos = 0;
    const run_size = elem_size + 4;
    while (pos + run_size <= data.length && out.length < row_count) {
        const v = decode_value(data, pos);
        pos += elem_size;
        const count = read_u32(data, pos);
        pos += 4;
        for (let k = 0; k < count && out.length < row_count; k++) out.push(v);
    }
    if (out.length !== row_count) {
        throw new Error(`RBCF: RLE decoded ${out.length} values, expected ${row_count}`);
    }
    return out;
}

// Varlen RLE: (value_len: u32, value_bytes, run_count: u32) triples.
function decode_rle_varlen(data: Uint8Array, row_count: number): Uint8Array[] {
    const out: Uint8Array[] = [];
    let pos = 0;
    while (out.length < row_count && pos + 4 <= data.length) {
        const value_len = read_u32(data, pos);
        pos += 4;
        if (pos + value_len + 4 > data.length) throw new Error("RBCF: varlen RLE truncated");
        const value = data.subarray(pos, pos + value_len);
        pos += value_len;
        const count = read_u32(data, pos);
        pos += 4;
        for (let k = 0; k < count && out.length < row_count; k++) out.push(value);
    }
    if (out.length !== row_count) {
        throw new Error(`RBCF: varlen RLE decoded ${out.length} values, expected ${row_count}`);
    }
    return out;
}

export function decode_rle(type_name: TypeName, row_count: number, data: Uint8Array): string[] {
    switch (type_name) {
        case "Int1":
            return decode_rle_fixed(data, row_count, 1, (b, p) => {
                const v = b[p];
                return (v > 0x7f ? v - 0x100 : v).toString();
            });
        case "Int2":
            return decode_rle_fixed(data, row_count, 2, (b, p) => read_i16(b, p).toString());
        case "Int4":
            return decode_rle_fixed(data, row_count, 4, (b, p) => read_i32(b, p).toString());
        case "Int8":
            return decode_rle_fixed(data, row_count, 8, (b, p) => read_i64(b, p).toString());
        case "Int16":
            return decode_rle_fixed(data, row_count, 16, (b, p) => read_i128(b, p).toString());
        case "Uint1":
            return decode_rle_fixed(data, row_count, 1, (b, p) => b[p].toString());
        case "Uint2":
            return decode_rle_fixed(data, row_count, 2, (b, p) => read_u16(b, p).toString());
        case "Uint4":
            return decode_rle_fixed(data, row_count, 4, (b, p) => read_u32(b, p).toString());
        case "Uint8":
            return decode_rle_fixed(data, row_count, 8, (b, p) => read_u64(b, p).toString());
        case "Uint16":
            return decode_rle_fixed(data, row_count, 16, (b, p) => read_u128(b, p).toString());
        case "Float4":
            return decode_rle_fixed(data, row_count, 4, (b, p) => format_f32(read_f32(b, p)));
        case "Float8":
            return decode_rle_fixed(data, row_count, 8, (b, p) => format_f64(read_f64(b, p)));
        case "Date":
            return decode_rle_fixed(data, row_count, 4, (b, p) => format_date(read_i32(b, p)));
        case "DateTime":
            return decode_rle_fixed(data, row_count, 8, (b, p) => format_date_time(read_u64(b, p)));
        case "Time":
            return decode_rle_fixed(data, row_count, 8, (b, p) => format_time(read_u64(b, p)));
        case "Int":
        case "Uint":
            return decode_rle_varlen(data, row_count).map((bytes) => signed_big_int_from_le_bytes(bytes).toString());
        case "Decimal":
            return decode_rle_varlen(data, row_count).map((bytes) => new TextDecoder("utf-8").decode(bytes));
        default:
            throw new Error(`RBCF: RLE not supported for type ${type_name}`);
    }
}
