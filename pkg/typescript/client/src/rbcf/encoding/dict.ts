// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Dict encoding: index array in `data`, dictionary table in `extra`.
// Port of crates/wire-format/src/encoding/dict.rs.

import { type TypeName } from "../format";
import { read_u16, read_u32 } from "../reader";
import { format_blob, signed_big_int_from_le_bytes } from "../values";

function read_index(data: Uint8Array, i: number, width: number): number {
    const off = i * width;
    switch (width) {
        case 1: return data[off];
        case 2: return read_u16(data, off);
        case 4: return read_u32(data, off);
        default: throw new Error(`RBCF: invalid dict index width ${width}`);
    }
}

interface DictTable {
    entries: Uint8Array[];
}

function decode_dict_table(extra: Uint8Array): DictTable {
    if (extra.length < 4) throw new Error("RBCF: dict table too short");
    const dict_count = read_u32(extra, 0);
    const offsets_start = 4;
    const offsets_end = offsets_start + (dict_count + 1) * 4;
    if (extra.length < offsets_end) throw new Error("RBCF: dict offsets truncated");
    const offsets = new Array<number>(dict_count + 1);
    for (let i = 0; i <= dict_count; i++) offsets[i] = read_u32(extra, offsets_start + i * 4);
    const entries = new Array<Uint8Array>(dict_count);
    const data_start = offsets_end;
    for (let i = 0; i < dict_count; i++) {
        entries[i] = extra.subarray(data_start + offsets[i], data_start + offsets[i + 1]);
    }
    return { entries };
}

export function decode_dict(
    type_name: TypeName,
    row_count: number,
    data: Uint8Array,
    extra: Uint8Array,
    index_width: number
): string[] {
    const table = decode_dict_table(extra);
    const out = new Array<string>(row_count);
    const decoder = new TextDecoder("utf-8");

    switch (type_name) {
        case "Utf8":
            for (let i = 0; i < row_count; i++) {
                const idx = read_index(data, i, index_width);
                if (idx >= table.entries.length) {
                    throw new Error(`RBCF: dict index ${idx} out of range (${table.entries.length} entries)`);
                }
                out[i] = decoder.decode(table.entries[idx]);
            }
            return out;
        case "Blob":
            for (let i = 0; i < row_count; i++) {
                const idx = read_index(data, i, index_width);
                out[i] = format_blob(table.entries[idx]);
            }
            return out;
        case "Int":
        case "Uint":
            for (let i = 0; i < row_count; i++) {
                const idx = read_index(data, i, index_width);
                out[i] = signed_big_int_from_le_bytes(table.entries[idx]).toString();
            }
            return out;
        case "Decimal":
            for (let i = 0; i < row_count; i++) {
                const idx = read_index(data, i, index_width);
                out[i] = decoder.decode(table.entries[idx]);
            }
            return out;
        default:
            throw new Error(`RBCF: Dict encoding not supported for type ${type_name}`);
    }
}
