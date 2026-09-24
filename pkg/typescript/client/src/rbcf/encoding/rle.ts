// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type TypeName } from "../format";
import {
    readF32, readF64, readI16, readI32, readI64, readI128, readI256,
    readU16, readU32, readU64, readU128,
} from "../reader";
import {
    formatDate, formatDateTime, formatF32, formatF64, formatTime,
} from "../values";

function decodeRleFixed<T>(
    data: Uint8Array,
    rowCount: number,
    elemSize: number,
    decodeValue: (buf: Uint8Array, pos: number) => T
): T[] {
    const out: T[] = [];
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
            return decodeRleFixed(data, rowCount, 8, (b, p) => formatDateTime(readI64(b, p)));
        case "Time":
            return decodeRleFixed(data, rowCount, 8, (b, p) => formatTime(readU64(b, p)));
        default:
            throw new Error(`RBCF: RLE not supported for type ${typeName}`);
    }
}

export function decodeRleFixedPoint(rowCount: number, data: Uint8Array, width: 16 | 32): bigint[] {
    return decodeRleFixed(data, rowCount, width, width === 16 ? readI128 : readI256);
}
