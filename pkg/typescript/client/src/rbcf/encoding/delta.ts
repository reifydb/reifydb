// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Delta and DeltaRLE encodings. Port of crates/wire-format/src/encoding/delta.rs.

import { type TypeName } from "../format";
import { read_i16, read_i32, read_i64, read_i128, read_u32, read_u64, read_u128 } from "../reader";
import { format_date, format_date_time, format_time } from "../values";

// Signed delta reader (width in bytes: 1/2/4/8), returns bigint for uniform arithmetic.
function read_signed_delta(data: Uint8Array, pos: number, width: number): bigint {
    switch (width) {
        case 1: {
            const v = data[pos];
            return BigInt(v > 0x7f ? v - 0x100 : v);
        }
        case 2: return BigInt(read_i16(data, pos));
        case 4: return BigInt(read_i32(data, pos));
        case 8: return read_i64(data, pos);
        default: throw new Error(`RBCF: invalid delta width ${width}`);
    }
}

// i128 delta reader (width 1/2/4/8/16), returns bigint.
function read_signed_delta_128(data: Uint8Array, pos: number, width: number): bigint {
    switch (width) {
        case 1: {
            const v = data[pos];
            return BigInt(v > 0x7f ? v - 0x100 : v);
        }
        case 2: return BigInt(read_i16(data, pos));
        case 4: return BigInt(read_i32(data, pos));
        case 8: return read_i64(data, pos);
        case 16: return read_i128(data, pos);
        default: throw new Error(`RBCF: invalid delta width ${width}`);
    }
}

// Two's-complement wrap for bigint at a given bit width.
function wrap_signed(v: bigint, bits: number): bigint {
    const mod = 1n << BigInt(bits);
    const half = 1n << BigInt(bits - 1);
    let w = v % mod;
    if (w < 0n) w += mod;
    return w >= half ? w - mod : w;
}

function wrap_unsigned(v: bigint, bits: number): bigint {
    const mod = 1n << BigInt(bits);
    let w = v % mod;
    if (w < 0n) w += mod;
    return w;
}

interface DeltaHeader {
    width: number;
    baseline: bigint;
    data_start: number;
}

function read_header_32(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 5) throw new Error("RBCF: delta header truncated (32)");
    const width = data[0];
    const baseline = signed ? BigInt(read_i32(data, 1)) : BigInt(read_u32(data, 1));
    return { width, baseline, data_start: 5 };
}

function read_header_64(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 9) throw new Error("RBCF: delta header truncated (64)");
    const width = data[0];
    const baseline = signed ? read_i64(data, 1) : read_u64(data, 1);
    return { width, baseline, data_start: 9 };
}

function read_header_128(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 17) throw new Error("RBCF: delta header truncated (128)");
    const width = data[0];
    const baseline = signed ? read_i128(data, 1) : read_u128(data, 1);
    return { width, baseline, data_start: 17 };
}

// Generic delta decode: reads (width) signed delta per subsequent value.
function decode_delta_generic(
    data: Uint8Array,
    row_count: number,
    header: DeltaHeader,
    read_delta: (data: Uint8Array, pos: number, width: number) => bigint,
    wrap: (v: bigint) => bigint
): bigint[] {
    if (row_count === 0) return [];
    const values = new Array<bigint>(row_count);
    values[0] = wrap(header.baseline);
    let pos = header.data_start;
    for (let i = 1; i < row_count; i++) {
        const d = read_delta(data, pos, header.width);
        pos += header.width;
        values[i] = wrap(values[i - 1] + d);
    }
    return values;
}

// Generic delta-RLE decode.
function decode_delta_rle_generic(
    data: Uint8Array,
    row_count: number,
    header: DeltaHeader,
    read_delta: (data: Uint8Array, pos: number, width: number) => bigint,
    wrap: (v: bigint) => bigint
): bigint[] {
    if (row_count === 0) return [];
    const values = new Array<bigint>(row_count);
    values[0] = wrap(header.baseline);
    let written = 1;
    let pos = header.data_start;
    while (written < row_count && pos + header.width + 4 <= data.length) {
        const d = read_delta(data, pos, header.width);
        pos += header.width;
        const count = read_u32(data, pos);
        pos += 4;
        for (let k = 0; k < count && written < row_count; k++) {
            values[written] = wrap(values[written - 1] + d);
            written++;
        }
    }
    if (written !== row_count) {
        throw new Error(`RBCF: delta_rle decoded ${written}, expected ${row_count}`);
    }
    return values;
}

export function decode_delta(type_name: TypeName, row_count: number, data: Uint8Array): string[] {
    return dispatch_delta(type_name, row_count, data, /* rle */ false);
}

export function decode_delta_rle(type_name: TypeName, row_count: number, data: Uint8Array): string[] {
    return dispatch_delta(type_name, row_count, data, /* rle */ true);
}

function dispatch_delta(type_name: TypeName, row_count: number, data: Uint8Array, rle: boolean): string[] {
    const go = rle ? decode_delta_rle_generic : decode_delta_generic;

    switch (type_name) {
        case "Int4": {
            const h = read_header_32(data, true);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_signed(v, 32));
            return vs.map((v) => v.toString());
        }
        case "Int8": {
            const h = read_header_64(data, true);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_signed(v, 64));
            return vs.map((v) => v.toString());
        }
        case "Uint8": {
            const h = read_header_64(data, false);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_unsigned(v, 64));
            return vs.map((v) => v.toString());
        }
        case "Int16": {
            const h = read_header_128(data, true);
            const vs = go(data, row_count, h, read_signed_delta_128, (v) => wrap_signed(v, 128));
            return vs.map((v) => v.toString());
        }
        case "Uint16": {
            const h = read_header_128(data, false);
            const vs = go(data, row_count, h, read_signed_delta_128, (v) => wrap_unsigned(v, 128));
            return vs.map((v) => v.toString());
        }
        case "Date": {
            const h = read_header_32(data, true);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_signed(v, 32));
            return vs.map((v) => format_date(Number(v)));
        }
        case "DateTime": {
            const h = read_header_64(data, false);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_unsigned(v, 64));
            return vs.map((v) => format_date_time(v));
        }
        case "Time": {
            const h = read_header_64(data, false);
            const vs = go(data, row_count, h, read_signed_delta, (v) => wrap_unsigned(v, 64));
            return vs.map((v) => format_time(v));
        }
        default:
            throw new Error(`RBCF: Delta not supported for type ${type_name}`);
    }
}
