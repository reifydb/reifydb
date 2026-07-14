// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Plain encoding: raw little-endian values for fixed-size types, offsets+data for varlen,
// bit-packed for booleans. Port of crates/codec/src/frame/encoding/plain.rs and the
// decode paths in crates/codec/src/frame/decode/{fixed,varlen,any}.rs.

import { NONE_VALUE } from "@reifydb/core";

import { TYPE_CODE, type TypeName } from "../format";
import {
    read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_i128,
    read_u16, read_u32, read_u64, read_u128,
} from "../reader";
import { decode_type_info } from "../typeinfo";
import {
    format_blob, format_date, format_date_time, format_duration, format_f32, format_f64,
    format_time, format_uuid, format_vector, signed_big_int_from_le_bytes,
} from "../values";
import { decode_bitvec } from "../nones";

export function decode_plain(
    type_name: TypeName,
    row_count: number,
    data: Uint8Array,
    offsets: Uint8Array
): string[] {
    const out = new Array<string>(row_count);
    switch (type_name) {
        case "Boolean": {
            const bits = decode_bitvec(data, row_count);
            for (let i = 0; i < row_count; i++) out[i] = bits[i] ? "true" : "false";
            return out;
        }
        case "Int1":
            for (let i = 0; i < row_count; i++) out[i] = read_i8(data, i).toString();
            return out;
        case "Int2":
            for (let i = 0; i < row_count; i++) out[i] = read_i16(data, i * 2).toString();
            return out;
        case "Int4":
            for (let i = 0; i < row_count; i++) out[i] = read_i32(data, i * 4).toString();
            return out;
        case "Int8":
            for (let i = 0; i < row_count; i++) out[i] = read_i64(data, i * 8).toString();
            return out;
        case "Int16":
            for (let i = 0; i < row_count; i++) out[i] = read_i128(data, i * 16).toString();
            return out;
        case "Uint1":
            for (let i = 0; i < row_count; i++) out[i] = data[i].toString();
            return out;
        case "Uint2":
            for (let i = 0; i < row_count; i++) out[i] = read_u16(data, i * 2).toString();
            return out;
        case "Uint4":
            for (let i = 0; i < row_count; i++) out[i] = read_u32(data, i * 4).toString();
            return out;
        case "Uint8":
            for (let i = 0; i < row_count; i++) out[i] = read_u64(data, i * 8).toString();
            return out;
        case "Uint16":
            for (let i = 0; i < row_count; i++) out[i] = read_u128(data, i * 16).toString();
            return out;
        case "Float4":
            for (let i = 0; i < row_count; i++) out[i] = format_f32(read_f32(data, i * 4));
            return out;
        case "Float8":
            for (let i = 0; i < row_count; i++) out[i] = format_f64(read_f64(data, i * 8));
            return out;
        case "Date":
            for (let i = 0; i < row_count; i++) out[i] = format_date(read_i32(data, i * 4));
            return out;
        case "DateTime":
            for (let i = 0; i < row_count; i++) out[i] = format_date_time(read_u64(data, i * 8));
            return out;
        case "Time":
            for (let i = 0; i < row_count; i++) out[i] = format_time(read_u64(data, i * 8));
            return out;
        case "Duration":
            for (let i = 0; i < row_count; i++) {
                const off = i * 16;
                const months = read_i32(data, off);
                const days = read_i32(data, off + 4);
                const nanos = read_i64(data, off + 8);
                out[i] = format_duration(months, days, nanos);
            }
            return out;
        case "IdentityId":
        case "Uuid4":
        case "Uuid7":
            for (let i = 0; i < row_count; i++) out[i] = format_uuid(data.subarray(i * 16, i * 16 + 16));
            return out;
        case "Utf8":
            return decode_varlen_strings(data, offsets, row_count);
        case "Blob":
            return decode_varlen_blobs(data, offsets, row_count);
        case "Vector":
            return decode_vectors(data, row_count);
        case "Int":
        case "Uint":
            return decode_varlen_big_numbers(data, offsets, row_count);
        case "Decimal":
            return decode_varlen_strings(data, offsets, row_count); // stored as UTF-8 decimal string
        case "Any":
            return decode_any(row_count, data);
        case "DictionaryId":
            return decode_dictionary_ids(row_count, data);
    }
    throw new Error(`RBCF: unsupported type in plain decode: ${type_name}`);
}

function decode_u32_offsets(offsets: Uint8Array, row_count: number): number[] {
    const result = new Array<number>(row_count + 1);
    for (let i = 0; i <= row_count; i++) result[i] = read_u32(offsets, i * 4);
    return result;
}

function decode_varlen_strings(data: Uint8Array, offsets: Uint8Array, row_count: number): string[] {
    const offs = decode_u32_offsets(offsets, row_count);
    const decoder = new TextDecoder("utf-8");
    const out = new Array<string>(row_count);
    for (let i = 0; i < row_count; i++) out[i] = decoder.decode(data.subarray(offs[i], offs[i + 1]));
    return out;
}

function decode_varlen_blobs(data: Uint8Array, offsets: Uint8Array, row_count: number): string[] {
    const offs = decode_u32_offsets(offsets, row_count);
    const out = new Array<string>(row_count);
    for (let i = 0; i < row_count; i++) out[i] = format_blob(data.subarray(offs[i], offs[i + 1]));
    return out;
}

// Vectors are fixed-stride per column, so they carry a u32 LE dims header instead of a
// per-row offsets array: [dims][row_count * dims * 4 bytes of LE f32].
// Port of crates/codec/src/frame/decode/vector.rs.
function decode_vectors(data: Uint8Array, row_count: number): string[] {
    if (data.length < 4) {
        throw new Error("RBCF: vector column is missing its 4-byte dimension header");
    }
    const dims = read_u32(data, 0);
    if (dims === 0) {
        throw new Error("RBCF: vector column declares zero dimensions");
    }
    const expected = row_count * dims * 4;
    if (data.length - 4 !== expected) {
        throw new Error(
            `RBCF: vector column payload is ${data.length - 4} bytes, expected ${expected} for ${row_count} rows of ${dims} dimensions`
        );
    }

    const out = new Array<string>(row_count);
    for (let i = 0; i < row_count; i++) {
        const base = 4 + i * dims * 4;
        const elements = new Array<number>(dims);
        for (let d = 0; d < dims; d++) elements[d] = read_f32(data, base + d * 4);
        out[i] = format_vector(elements);
    }
    return out;
}

function decode_varlen_big_numbers(data: Uint8Array, offsets: Uint8Array, row_count: number): string[] {
    const offs = decode_u32_offsets(offsets, row_count);
    const out = new Array<string>(row_count);
    for (let i = 0; i < row_count; i++) {
        const slice = data.subarray(offs[i], offs[i + 1]);
        out[i] = signed_big_int_from_le_bytes(slice).toString();
    }
    return out;
}

function decode_any(row_count: number, data: Uint8Array): string[] {
    const out = new Array<string>(row_count);
    let pos = 0;
    for (let i = 0; i < row_count; i++) {
        const { value, next_pos } = decode_any_value(data, pos);
        out[i] = value;
        pos = next_pos;
    }
    return out;
}

// Port of crates/codec/src/value/mod.rs decode_value_from: kind byte + payload.
// None values (kind 0x00 + typeinfo of the inner type) decode to the NONE_VALUE
// sentinel; List/Record/Tuple render in the same form as Rust's Value Display.
export function decode_any_value(data: Uint8Array, pos: number): { value: string; next_pos: number } {
    const tag = data[pos];
    pos += 1;
    switch (tag) {
        case TYPE_CODE.None: {
            const inner = decode_type_info(data, pos);
            return { value: NONE_VALUE, next_pos: inner.next_pos };
        }
        case TYPE_CODE.Boolean:
            return { value: data[pos] !== 0 ? "true" : "false", next_pos: pos + 1 };
        case TYPE_CODE.Float4:
            return { value: format_f32(read_f32(data, pos)), next_pos: pos + 4 };
        case TYPE_CODE.Float8:
            return { value: format_f64(read_f64(data, pos)), next_pos: pos + 8 };
        case TYPE_CODE.Int1:
            return { value: read_i8(data, pos).toString(), next_pos: pos + 1 };
        case TYPE_CODE.Int2:
            return { value: read_i16(data, pos).toString(), next_pos: pos + 2 };
        case TYPE_CODE.Int4:
            return { value: read_i32(data, pos).toString(), next_pos: pos + 4 };
        case TYPE_CODE.Int8:
            return { value: read_i64(data, pos).toString(), next_pos: pos + 8 };
        case TYPE_CODE.Int16:
            return { value: read_i128(data, pos).toString(), next_pos: pos + 16 };
        case TYPE_CODE.Uint1:
            return { value: data[pos].toString(), next_pos: pos + 1 };
        case TYPE_CODE.Uint2:
            return { value: read_u16(data, pos).toString(), next_pos: pos + 2 };
        case TYPE_CODE.Uint4:
            return { value: read_u32(data, pos).toString(), next_pos: pos + 4 };
        case TYPE_CODE.Uint8:
            return { value: read_u64(data, pos).toString(), next_pos: pos + 8 };
        case TYPE_CODE.Uint16:
            return { value: read_u128(data, pos).toString(), next_pos: pos + 16 };
        case TYPE_CODE.Date:
            return { value: format_date(read_i32(data, pos)), next_pos: pos + 4 };
        case TYPE_CODE.DateTime:
            return { value: format_date_time(read_u64(data, pos)), next_pos: pos + 8 };
        case TYPE_CODE.Time:
            return { value: format_time(read_u64(data, pos)), next_pos: pos + 8 };
        case TYPE_CODE.Duration: {
            const months = read_i32(data, pos);
            const days = read_i32(data, pos + 4);
            const nanos = read_i64(data, pos + 8);
            return { value: format_duration(months, days, nanos), next_pos: pos + 16 };
        }
        case TYPE_CODE.IdentityId:
        case TYPE_CODE.Uuid4:
        case TYPE_CODE.Uuid7:
            return { value: format_uuid(data.subarray(pos, pos + 16)), next_pos: pos + 16 };
        case TYPE_CODE.Utf8: {
            const len = read_u32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: new TextDecoder("utf-8").decode(slice), next_pos: pos + 4 + len };
        }
        case TYPE_CODE.Blob: {
            const len = read_u32(data, pos);
            return { value: format_blob(data.subarray(pos + 4, pos + 4 + len)), next_pos: pos + 4 + len };
        }
        case TYPE_CODE.Vector: {
            // Length prefix is a BYTE count, not an element count.
            const len = read_u32(data, pos);
            if (len % 4 !== 0) {
                throw new Error(`RBCF: vector payload of ${len} bytes is not a whole number of f32 elements`);
            }
            const count = len / 4;
            const elements = new Array<number>(count);
            for (let i = 0; i < count; i++) elements[i] = read_f32(data, pos + 4 + i * 4);
            return { value: format_vector(elements), next_pos: pos + 4 + len };
        }
        case TYPE_CODE.Int:
        case TYPE_CODE.Uint: {
            const len = read_u32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: signed_big_int_from_le_bytes(slice).toString(), next_pos: pos + 4 + len };
        }
        case TYPE_CODE.Decimal: {
            const len = read_u32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: new TextDecoder("utf-8").decode(slice), next_pos: pos + 4 + len };
        }
        case TYPE_CODE.Any:
            return decode_any_value(data, pos);
        case TYPE_CODE.DictionaryId: {
            const width = data[pos];
            pos += 1;
            let v: bigint | number;
            switch (width) {
                case 1: v = data[pos]; break;
                case 2: v = read_u16(data, pos); break;
                case 4: v = read_u32(data, pos); break;
                case 8: v = read_u64(data, pos); break;
                case 16: v = read_u128(data, pos); break;
                default: throw new Error(`RBCF: invalid DictionaryId width ${width}`);
            }
            return { value: v.toString(), next_pos: pos + width };
        }
        case TYPE_CODE.Type: {
            const info = decode_type_info(data, pos);
            return { value: info.name, next_pos: info.next_pos };
        }
        case TYPE_CODE.List:
        case TYPE_CODE.Tuple: {
            const count = read_u32(data, pos);
            pos += 4;
            const items = new Array<string>(count);
            for (let i = 0; i < count; i++) {
                const item = decode_any_value(data, pos);
                items[i] = item.value;
                pos = item.next_pos;
            }
            const open = tag === TYPE_CODE.List ? "[" : "(";
            const close = tag === TYPE_CODE.List ? "]" : ")";
            return { value: `${open}${items.join(", ")}${close}`, next_pos: pos };
        }
        case TYPE_CODE.Record: {
            const count = read_u32(data, pos);
            pos += 4;
            const fields = new Array<string>(count);
            for (let i = 0; i < count; i++) {
                const key_len = read_u32(data, pos);
                pos += 4;
                const key = new TextDecoder("utf-8").decode(data.subarray(pos, pos + key_len));
                pos += key_len;
                const field = decode_any_value(data, pos);
                pos = field.next_pos;
                fields[i] = `${key}: ${field.value}`;
            }
            return { value: `{${fields.join(", ")}}`, next_pos: pos };
        }
        default:
            throw new Error(`RBCF: unsupported Any type tag: ${tag}`);
    }
}

function decode_dictionary_ids(row_count: number, data: Uint8Array): string[] {
    if (row_count === 0 || data.length === 0) return [];
    const disc = data[0];
    const out = new Array<string>(row_count);
    let pos = 1;
    for (let i = 0; i < row_count; i++) {
        let v: bigint | number;
        switch (disc) {
            case 1:
                v = data[pos];
                pos += 1;
                break;
            case 2:
                v = read_u16(data, pos);
                pos += 2;
                break;
            case 4:
                v = read_u32(data, pos);
                pos += 4;
                break;
            case 8:
                v = read_u64(data, pos);
                pos += 8;
                break;
            case 16:
                v = read_u128(data, pos);
                pos += 16;
                break;
            default:
                throw new Error(`RBCF: invalid DictionaryId discriminator ${disc}`);
        }
        out[i] = v.toString();
    }
    return out;
}
