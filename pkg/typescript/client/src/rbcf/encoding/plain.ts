// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Plain encoding: raw little-endian values for fixed-size types, offsets+data for varlen,
// bit-packed for booleans. Port of crates/wire-format/src/encoding/plain.rs and the
// decode paths in crates/wire-format/src/decode/{fixed,varlen,any}.rs.

import { TYPE_CODE, type TypeName } from "../format";
import {
    readF32, readF64, readI8, readI16, readI32, readI64, readI128,
    readU16, readU32, readU64, readU128,
} from "../reader";
import {
    formatBlob, formatDate, formatDateTime, formatDuration, formatF32, formatF64,
    formatTime, formatUuid, signedBigIntFromLeBytes,
} from "../values";
import { decodeBitvec } from "../nones";

export function decodePlain(
    typeName: TypeName,
    rowCount: number,
    data: Uint8Array,
    offsets: Uint8Array
): string[] {
    const out = new Array<string>(rowCount);
    switch (typeName) {
        case "Boolean": {
            const bits = decodeBitvec(data, rowCount);
            for (let i = 0; i < rowCount; i++) out[i] = bits[i] ? "true" : "false";
            return out;
        }
        case "Int1":
            for (let i = 0; i < rowCount; i++) out[i] = readI8(data, i).toString();
            return out;
        case "Int2":
            for (let i = 0; i < rowCount; i++) out[i] = readI16(data, i * 2).toString();
            return out;
        case "Int4":
            for (let i = 0; i < rowCount; i++) out[i] = readI32(data, i * 4).toString();
            return out;
        case "Int8":
            for (let i = 0; i < rowCount; i++) out[i] = readI64(data, i * 8).toString();
            return out;
        case "Int16":
            for (let i = 0; i < rowCount; i++) out[i] = readI128(data, i * 16).toString();
            return out;
        case "Uint1":
            for (let i = 0; i < rowCount; i++) out[i] = data[i].toString();
            return out;
        case "Uint2":
            for (let i = 0; i < rowCount; i++) out[i] = readU16(data, i * 2).toString();
            return out;
        case "Uint4":
            for (let i = 0; i < rowCount; i++) out[i] = readU32(data, i * 4).toString();
            return out;
        case "Uint8":
            for (let i = 0; i < rowCount; i++) out[i] = readU64(data, i * 8).toString();
            return out;
        case "Uint16":
            for (let i = 0; i < rowCount; i++) out[i] = readU128(data, i * 16).toString();
            return out;
        case "Float4":
            for (let i = 0; i < rowCount; i++) out[i] = formatF32(readF32(data, i * 4));
            return out;
        case "Float8":
            for (let i = 0; i < rowCount; i++) out[i] = formatF64(readF64(data, i * 8));
            return out;
        case "Date":
            for (let i = 0; i < rowCount; i++) out[i] = formatDate(readI32(data, i * 4));
            return out;
        case "DateTime":
            for (let i = 0; i < rowCount; i++) out[i] = formatDateTime(readU64(data, i * 8));
            return out;
        case "Time":
            for (let i = 0; i < rowCount; i++) out[i] = formatTime(readU64(data, i * 8));
            return out;
        case "Duration":
            for (let i = 0; i < rowCount; i++) {
                const off = i * 16;
                const months = readI32(data, off);
                const days = readI32(data, off + 4);
                const nanos = readI64(data, off + 8);
                out[i] = formatDuration(months, days, nanos);
            }
            return out;
        case "IdentityId":
        case "Uuid4":
        case "Uuid7":
            for (let i = 0; i < rowCount; i++) out[i] = formatUuid(data.subarray(i * 16, i * 16 + 16));
            return out;
        case "Utf8":
            return decodeVarlenStrings(data, offsets, rowCount);
        case "Blob":
            return decodeVarlenBlobs(data, offsets, rowCount);
        case "Int":
        case "Uint":
            return decodeVarlenBigNumbers(data, offsets, rowCount);
        case "Decimal":
            return decodeVarlenStrings(data, offsets, rowCount); // stored as UTF-8 decimal string
        case "Any":
            return decodeAny(rowCount, data);
        case "DictionaryId":
            return decodeDictionaryIds(rowCount, data);
    }
    throw new Error(`RBCF: unsupported type in plain decode: ${typeName}`);
}

function decodeU32Offsets(offsets: Uint8Array, rowCount: number): number[] {
    const result = new Array<number>(rowCount + 1);
    for (let i = 0; i <= rowCount; i++) result[i] = readU32(offsets, i * 4);
    return result;
}

function decodeVarlenStrings(data: Uint8Array, offsets: Uint8Array, rowCount: number): string[] {
    const offs = decodeU32Offsets(offsets, rowCount);
    const decoder = new TextDecoder("utf-8");
    const out = new Array<string>(rowCount);
    for (let i = 0; i < rowCount; i++) out[i] = decoder.decode(data.subarray(offs[i], offs[i + 1]));
    return out;
}

function decodeVarlenBlobs(data: Uint8Array, offsets: Uint8Array, rowCount: number): string[] {
    const offs = decodeU32Offsets(offsets, rowCount);
    const out = new Array<string>(rowCount);
    for (let i = 0; i < rowCount; i++) out[i] = formatBlob(data.subarray(offs[i], offs[i + 1]));
    return out;
}

function decodeVarlenBigNumbers(data: Uint8Array, offsets: Uint8Array, rowCount: number): string[] {
    const offs = decodeU32Offsets(offsets, rowCount);
    const out = new Array<string>(rowCount);
    for (let i = 0; i < rowCount; i++) {
        const slice = data.subarray(offs[i], offs[i + 1]);
        out[i] = signedBigIntFromLeBytes(slice).toString();
    }
    return out;
}

function decodeAny(rowCount: number, data: Uint8Array): string[] {
    const out = new Array<string>(rowCount);
    let pos = 0;
    for (let i = 0; i < rowCount; i++) {
        const { value, nextPos } = decodeAnyValue(data, pos);
        out[i] = value;
        pos = nextPos;
    }
    return out;
}

// Port of crates/wire-format/src/decode/any.rs: type_tag + value bytes.
function decodeAnyValue(data: Uint8Array, pos: number): { value: string; nextPos: number } {
    const tag = data[pos];
    pos += 1;
    // Tag is base type code (no option bit here).
    switch (tag) {
        case TYPE_CODE.Boolean:
            return { value: data[pos] !== 0 ? "true" : "false", nextPos: pos + 1 };
        case TYPE_CODE.Float4:
            return { value: formatF32(readF32(data, pos)), nextPos: pos + 4 };
        case TYPE_CODE.Float8:
            return { value: formatF64(readF64(data, pos)), nextPos: pos + 8 };
        case TYPE_CODE.Int1:
            return { value: readI8(data, pos).toString(), nextPos: pos + 1 };
        case TYPE_CODE.Int2:
            return { value: readI16(data, pos).toString(), nextPos: pos + 2 };
        case TYPE_CODE.Int4:
            return { value: readI32(data, pos).toString(), nextPos: pos + 4 };
        case TYPE_CODE.Int8:
            return { value: readI64(data, pos).toString(), nextPos: pos + 8 };
        case TYPE_CODE.Int16:
            return { value: readI128(data, pos).toString(), nextPos: pos + 16 };
        case TYPE_CODE.Uint1:
            return { value: data[pos].toString(), nextPos: pos + 1 };
        case TYPE_CODE.Uint2:
            return { value: readU16(data, pos).toString(), nextPos: pos + 2 };
        case TYPE_CODE.Uint4:
            return { value: readU32(data, pos).toString(), nextPos: pos + 4 };
        case TYPE_CODE.Uint8:
            return { value: readU64(data, pos).toString(), nextPos: pos + 8 };
        case TYPE_CODE.Uint16:
            return { value: readU128(data, pos).toString(), nextPos: pos + 16 };
        case TYPE_CODE.Date:
            return { value: formatDate(readI32(data, pos)), nextPos: pos + 4 };
        case TYPE_CODE.DateTime:
            return { value: formatDateTime(readU64(data, pos)), nextPos: pos + 8 };
        case TYPE_CODE.Time:
            return { value: formatTime(readU64(data, pos)), nextPos: pos + 8 };
        case TYPE_CODE.Duration: {
            const months = readI32(data, pos);
            const days = readI32(data, pos + 4);
            const nanos = readI64(data, pos + 8);
            return { value: formatDuration(months, days, nanos), nextPos: pos + 16 };
        }
        case TYPE_CODE.IdentityId:
        case TYPE_CODE.Uuid4:
        case TYPE_CODE.Uuid7:
            return { value: formatUuid(data.subarray(pos, pos + 16)), nextPos: pos + 16 };
        case TYPE_CODE.Utf8: {
            const len = readU32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: new TextDecoder("utf-8").decode(slice), nextPos: pos + 4 + len };
        }
        case TYPE_CODE.Blob: {
            const len = readU32(data, pos);
            return { value: formatBlob(data.subarray(pos + 4, pos + 4 + len)), nextPos: pos + 4 + len };
        }
        case TYPE_CODE.Int:
        case TYPE_CODE.Uint: {
            const len = readU32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: signedBigIntFromLeBytes(slice).toString(), nextPos: pos + 4 + len };
        }
        case TYPE_CODE.Decimal: {
            const len = readU32(data, pos);
            const slice = data.subarray(pos + 4, pos + 4 + len);
            return { value: new TextDecoder("utf-8").decode(slice), nextPos: pos + 4 + len };
        }
        default:
            throw new Error(`RBCF: unsupported Any type tag: ${tag}`);
    }
}

function decodeDictionaryIds(rowCount: number, data: Uint8Array): string[] {
    if (rowCount === 0 || data.length === 0) return [];
    const disc = data[0];
    const out = new Array<string>(rowCount);
    let pos = 1;
    for (let i = 0; i < rowCount; i++) {
        let v: bigint | number;
        switch (disc) {
            case 1:
                v = data[pos];
                pos += 1;
                break;
            case 2:
                v = readU16(data, pos);
                pos += 2;
                break;
            case 4:
                v = readU32(data, pos);
                pos += 4;
                break;
            case 8:
                v = readU64(data, pos);
                pos += 8;
                break;
            case 16:
                v = readU128(data, pos);
                pos += 16;
                break;
            default:
                throw new Error(`RBCF: invalid DictionaryId discriminator ${disc}`);
        }
        out[i] = v.toString();
    }
    return out;
}
