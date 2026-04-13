// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Run-length encoding. Port of crates/wire-format/src/encoding/rle.rs and
// the dispatch in crates/wire-format/src/decode/fixed.rs decode_rle_column.

import { type TypeName } from "../format";
import {
    readF32, readF64, readI16, readI32, readI64, readI128,
    readU16, readU32, readU64, readU128,
} from "../reader";
import {
    formatDate, formatDateTime, formatF32, formatF64, formatTime, signedBigIntFromLeBytes,
} from "../values";

// Fixed-size RLE: (value, u32 run_length) pairs.
function decodeRleFixed(
    data: Uint8Array,
    rowCount: number,
    elemSize: number,
    decodeValue: (buf: Uint8Array, pos: number) => string
): string[] {
    const out: string[] = [];
    let pos = 0;
    const runSize = elemSize + 4;
    while (pos + runSize <= data.length && out.length < rowCount) {
        const v = decodeValue(data, pos);
        pos += elemSize;
        const count = readU32(data, pos);
        pos += 4;
        for (let k = 0; k < count && out.length < rowCount; k++) out.push(v);
    }
    if (out.length !== rowCount) {
        throw new Error(`RBCF: RLE decoded ${out.length} values, expected ${rowCount}`);
    }
    return out;
}

// Varlen RLE: (value_len: u32, value_bytes, run_count: u32) triples.
function decodeRleVarlen(data: Uint8Array, rowCount: number): Uint8Array[] {
    const out: Uint8Array[] = [];
    let pos = 0;
    while (out.length < rowCount && pos + 4 <= data.length) {
        const valueLen = readU32(data, pos);
        pos += 4;
        if (pos + valueLen + 4 > data.length) throw new Error("RBCF: varlen RLE truncated");
        const value = data.subarray(pos, pos + valueLen);
        pos += valueLen;
        const count = readU32(data, pos);
        pos += 4;
        for (let k = 0; k < count && out.length < rowCount; k++) out.push(value);
    }
    if (out.length !== rowCount) {
        throw new Error(`RBCF: varlen RLE decoded ${out.length} values, expected ${rowCount}`);
    }
    return out;
}

export function decodeRle(typeName: TypeName, rowCount: number, data: Uint8Array): string[] {
    switch (typeName) {
        case "Int1":
            return decodeRleFixed(data, rowCount, 1, (b, p) => {
                const v = b[p];
                return (v > 0x7f ? v - 0x100 : v).toString();
            });
        case "Int2":
            return decodeRleFixed(data, rowCount, 2, (b, p) => readI16(b, p).toString());
        case "Int4":
            return decodeRleFixed(data, rowCount, 4, (b, p) => readI32(b, p).toString());
        case "Int8":
            return decodeRleFixed(data, rowCount, 8, (b, p) => readI64(b, p).toString());
        case "Int16":
            return decodeRleFixed(data, rowCount, 16, (b, p) => readI128(b, p).toString());
        case "Uint1":
            return decodeRleFixed(data, rowCount, 1, (b, p) => b[p].toString());
        case "Uint2":
            return decodeRleFixed(data, rowCount, 2, (b, p) => readU16(b, p).toString());
        case "Uint4":
            return decodeRleFixed(data, rowCount, 4, (b, p) => readU32(b, p).toString());
        case "Uint8":
            return decodeRleFixed(data, rowCount, 8, (b, p) => readU64(b, p).toString());
        case "Uint16":
            return decodeRleFixed(data, rowCount, 16, (b, p) => readU128(b, p).toString());
        case "Float4":
            return decodeRleFixed(data, rowCount, 4, (b, p) => formatF32(readF32(b, p)));
        case "Float8":
            return decodeRleFixed(data, rowCount, 8, (b, p) => formatF64(readF64(b, p)));
        case "Date":
            return decodeRleFixed(data, rowCount, 4, (b, p) => formatDate(readI32(b, p)));
        case "DateTime":
            return decodeRleFixed(data, rowCount, 8, (b, p) => formatDateTime(readU64(b, p)));
        case "Time":
            return decodeRleFixed(data, rowCount, 8, (b, p) => formatTime(readU64(b, p)));
        case "Int":
        case "Uint":
            return decodeRleVarlen(data, rowCount).map((bytes) => signedBigIntFromLeBytes(bytes).toString());
        case "Decimal":
            return decodeRleVarlen(data, rowCount).map((bytes) => new TextDecoder("utf-8").decode(bytes));
        default:
            throw new Error(`RBCF: RLE not supported for type ${typeName}`);
    }
}
