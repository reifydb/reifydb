// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Dict encoding: index array in `data`, dictionary table in `extra`.
// Port of crates/wire-format/src/encoding/dict.rs.

import { type TypeName } from "../format";
import { readU16, readU32 } from "../reader";
import { formatBlob, signedBigIntFromLeBytes } from "../values";

function readIndex(data: Uint8Array, i: number, width: number): number {
    const off = i * width;
    switch (width) {
        case 1: return data[off];
        case 2: return readU16(data, off);
        case 4: return readU32(data, off);
        default: throw new Error(`RBCF: invalid dict index width ${width}`);
    }
}

interface DictTable {
    entries: Uint8Array[];
}

function decodeDictTable(extra: Uint8Array): DictTable {
    if (extra.length < 4) throw new Error("RBCF: dict table too short");
    const dictCount = readU32(extra, 0);
    const offsetsStart = 4;
    const offsetsEnd = offsetsStart + (dictCount + 1) * 4;
    if (extra.length < offsetsEnd) throw new Error("RBCF: dict offsets truncated");
    const offsets = new Array<number>(dictCount + 1);
    for (let i = 0; i <= dictCount; i++) offsets[i] = readU32(extra, offsetsStart + i * 4);
    const entries = new Array<Uint8Array>(dictCount);
    const dataStart = offsetsEnd;
    for (let i = 0; i < dictCount; i++) {
        entries[i] = extra.subarray(dataStart + offsets[i], dataStart + offsets[i + 1]);
    }
    return { entries };
}

export function decodeDict(
    typeName: TypeName,
    rowCount: number,
    data: Uint8Array,
    extra: Uint8Array,
    indexWidth: number
): string[] {
    const table = decodeDictTable(extra);
    const out = new Array<string>(rowCount);
    const decoder = new TextDecoder("utf-8");

    switch (typeName) {
        case "Utf8":
            for (let i = 0; i < rowCount; i++) {
                const idx = readIndex(data, i, indexWidth);
                if (idx >= table.entries.length) {
                    throw new Error(`RBCF: dict index ${idx} out of range (${table.entries.length} entries)`);
                }
                out[i] = decoder.decode(table.entries[idx]);
            }
            return out;
        case "Blob":
            for (let i = 0; i < rowCount; i++) {
                const idx = readIndex(data, i, indexWidth);
                out[i] = formatBlob(table.entries[idx]);
            }
            return out;
        case "Int":
        case "Uint":
            for (let i = 0; i < rowCount; i++) {
                const idx = readIndex(data, i, indexWidth);
                out[i] = signedBigIntFromLeBytes(table.entries[idx]).toString();
            }
            return out;
        case "Decimal":
            for (let i = 0; i < rowCount; i++) {
                const idx = readIndex(data, i, indexWidth);
                out[i] = decoder.decode(table.entries[idx]);
            }
            return out;
        default:
            throw new Error(`RBCF: Dict encoding not supported for type ${typeName}`);
    }
}
