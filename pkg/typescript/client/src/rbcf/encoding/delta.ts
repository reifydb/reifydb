// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Delta and DeltaRLE encodings. Port of crates/wire-format/src/encoding/delta.rs.

import { type TypeName } from "../format";
import { readI16, readI32, readI64, readI128, readU32, readU64, readU128 } from "../reader";
import { formatDate, formatDateTime, formatTime } from "../values";

// Signed delta reader (width in bytes: 1/2/4/8), returns bigint for uniform arithmetic.
function readSignedDelta(data: Uint8Array, pos: number, width: number): bigint {
    switch (width) {
        case 1: {
            const v = data[pos];
            return BigInt(v > 0x7f ? v - 0x100 : v);
        }
        case 2: return BigInt(readI16(data, pos));
        case 4: return BigInt(readI32(data, pos));
        case 8: return readI64(data, pos);
        default: throw new Error(`RBCF: invalid delta width ${width}`);
    }
}

// i128 delta reader (width 1/2/4/8/16), returns bigint.
function readSignedDelta128(data: Uint8Array, pos: number, width: number): bigint {
    switch (width) {
        case 1: {
            const v = data[pos];
            return BigInt(v > 0x7f ? v - 0x100 : v);
        }
        case 2: return BigInt(readI16(data, pos));
        case 4: return BigInt(readI32(data, pos));
        case 8: return readI64(data, pos);
        case 16: return readI128(data, pos);
        default: throw new Error(`RBCF: invalid delta width ${width}`);
    }
}

// Two's-complement wrap for bigint at a given bit width.
function wrapSigned(v: bigint, bits: number): bigint {
    const mod = 1n << BigInt(bits);
    const half = 1n << BigInt(bits - 1);
    let w = v % mod;
    if (w < 0n) w += mod;
    return w >= half ? w - mod : w;
}

function wrapUnsigned(v: bigint, bits: number): bigint {
    const mod = 1n << BigInt(bits);
    let w = v % mod;
    if (w < 0n) w += mod;
    return w;
}

interface DeltaHeader {
    width: number;
    baseline: bigint;
    dataStart: number;
}

function readHeader32(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 5) throw new Error("RBCF: delta header truncated (32)");
    const width = data[0];
    const baseline = signed ? BigInt(readI32(data, 1)) : BigInt(readU32(data, 1));
    return { width, baseline, dataStart: 5 };
}

function readHeader64(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 9) throw new Error("RBCF: delta header truncated (64)");
    const width = data[0];
    const baseline = signed ? readI64(data, 1) : readU64(data, 1);
    return { width, baseline, dataStart: 9 };
}

function readHeader128(data: Uint8Array, signed: boolean): DeltaHeader {
    if (data.length < 17) throw new Error("RBCF: delta header truncated (128)");
    const width = data[0];
    const baseline = signed ? readI128(data, 1) : readU128(data, 1);
    return { width, baseline, dataStart: 17 };
}

// Generic delta decode: reads (width) signed delta per subsequent value.
function decodeDeltaGeneric(
    data: Uint8Array,
    rowCount: number,
    header: DeltaHeader,
    readDelta: (data: Uint8Array, pos: number, width: number) => bigint,
    wrap: (v: bigint) => bigint
): bigint[] {
    if (rowCount === 0) return [];
    const values = new Array<bigint>(rowCount);
    values[0] = wrap(header.baseline);
    let pos = header.dataStart;
    for (let i = 1; i < rowCount; i++) {
        const d = readDelta(data, pos, header.width);
        pos += header.width;
        values[i] = wrap(values[i - 1] + d);
    }
    return values;
}

// Generic delta-RLE decode.
function decodeDeltaRleGeneric(
    data: Uint8Array,
    rowCount: number,
    header: DeltaHeader,
    readDelta: (data: Uint8Array, pos: number, width: number) => bigint,
    wrap: (v: bigint) => bigint
): bigint[] {
    if (rowCount === 0) return [];
    const values = new Array<bigint>(rowCount);
    values[0] = wrap(header.baseline);
    let written = 1;
    let pos = header.dataStart;
    while (written < rowCount && pos + header.width + 4 <= data.length) {
        const d = readDelta(data, pos, header.width);
        pos += header.width;
        const count = readU32(data, pos);
        pos += 4;
        for (let k = 0; k < count && written < rowCount; k++) {
            values[written] = wrap(values[written - 1] + d);
            written++;
        }
    }
    if (written !== rowCount) {
        throw new Error(`RBCF: delta_rle decoded ${written}, expected ${rowCount}`);
    }
    return values;
}

export function decodeDelta(typeName: TypeName, rowCount: number, data: Uint8Array): string[] {
    return dispatchDelta(typeName, rowCount, data, /* rle */ false);
}

export function decodeDeltaRle(typeName: TypeName, rowCount: number, data: Uint8Array): string[] {
    return dispatchDelta(typeName, rowCount, data, /* rle */ true);
}

function dispatchDelta(typeName: TypeName, rowCount: number, data: Uint8Array, rle: boolean): string[] {
    const go = rle ? decodeDeltaRleGeneric : decodeDeltaGeneric;

    switch (typeName) {
        case "Int4": {
            const h = readHeader32(data, true);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapSigned(v, 32));
            return vs.map((v) => v.toString());
        }
        case "Int8": {
            const h = readHeader64(data, true);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapSigned(v, 64));
            return vs.map((v) => v.toString());
        }
        case "Uint8": {
            const h = readHeader64(data, false);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapUnsigned(v, 64));
            return vs.map((v) => v.toString());
        }
        case "Int16": {
            const h = readHeader128(data, true);
            const vs = go(data, rowCount, h, readSignedDelta128, (v) => wrapSigned(v, 128));
            return vs.map((v) => v.toString());
        }
        case "Uint16": {
            const h = readHeader128(data, false);
            const vs = go(data, rowCount, h, readSignedDelta128, (v) => wrapUnsigned(v, 128));
            return vs.map((v) => v.toString());
        }
        case "Date": {
            const h = readHeader32(data, true);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapSigned(v, 32));
            return vs.map((v) => formatDate(Number(v)));
        }
        case "DateTime": {
            const h = readHeader64(data, false);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapUnsigned(v, 64));
            return vs.map((v) => formatDateTime(v));
        }
        case "Time": {
            const h = readHeader64(data, false);
            const vs = go(data, rowCount, h, readSignedDelta, (v) => wrapUnsigned(v, 64));
            return vs.map((v) => formatTime(v));
        }
        default:
            throw new Error(`RBCF: Delta not supported for type ${typeName}`);
    }
}
