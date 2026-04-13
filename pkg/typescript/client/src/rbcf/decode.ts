// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// RBCF message decoder: Uint8Array -> WireFrame[].
// Port of crates/wire-format/src/decode/mod.rs.

import type { Type } from "@reifydb/core";
import { NONE_VALUE } from "@reifydb/core";

import {
    COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, ColumnEncoding, FRAME_HEADER_SIZE,
    META_HAS_CREATED_AT, META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT, MESSAGE_HEADER_SIZE,
    RBCF_MAGIC, RBCF_VERSION, dictIndexWidthFromFlags, isOptionCode, typeNameFromCode,
} from "./format";
import { BinaryReader } from "./reader";
import { decodeBitvec } from "./nones";
import { formatDateTime } from "./values";
import type { WireColumn, WireFrame } from "./types";
import { decodePlain } from "./encoding/plain";
import { decodeDict } from "./encoding/dict";
import { decodeRle } from "./encoding/rle";
import { decodeDelta, decodeDeltaRle } from "./encoding/delta";

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
    const frameCount = r.u32();
    r.u32(); // total_size (trusted; we stop at EOF if malformed)

    const frames: WireFrame[] = [];
    for (let i = 0; i < frameCount; i++) frames.push(decodeFrame(r));
    return frames;
}

function decodeFrame(r: BinaryReader): WireFrame {
    const frameStart = r.pos;
    if (r.remaining() < FRAME_HEADER_SIZE) throw new Error("RBCF: frame header truncated");

    const rowCount = r.u32();
    const colCount = r.u16();
    const metaFlags = r.u8();
    r.u8(); // reserved
    r.u32(); // frame_size
    void frameStart;

    const frame: WireFrame = { columns: [] };

    if (metaFlags & META_HAS_ROW_NUMBERS) {
        const rows = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) rows[i] = r.u64().toString();
        frame.row_numbers = rows;
    }
    if (metaFlags & META_HAS_CREATED_AT) {
        const cr = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) cr[i] = formatDateTime(r.u64());
        frame.created_at = cr;
    }
    if (metaFlags & META_HAS_UPDATED_AT) {
        const up = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) up[i] = formatDateTime(r.u64());
        frame.updated_at = up;
    }

    for (let c = 0; c < colCount; c++) frame.columns.push(decodeColumn(r));
    return frame;
}

function decodeColumn(r: BinaryReader): WireColumn {
    if (r.remaining() < COLUMN_DESCRIPTOR_SIZE) throw new Error("RBCF: column descriptor truncated");

    const typeCode = r.u8();
    const encodingByte = r.u8();
    const flags = r.u8();
    r.u8(); // reserved
    const nameLen = r.u16();
    r.u16(); // reserved2
    const rowCount = r.u32();
    const nonesLen = r.u32();
    const dataLen = r.u32();
    const offsetsLen = r.u32();
    const extraLen = r.u32();

    const encoding = encodingByte as ColumnEncoding;
    const hasNones = (flags & COL_FLAG_HAS_NONES) !== 0;
    const optionOuter = isOptionCode(typeCode);

    const name = r.utf8(nameLen);
    const namePad = (4 - (nameLen % 4)) % 4;
    if (namePad > 0) r.skip(namePad);

    // Slice each segment.
    const nonesBytes = r.bytes(nonesLen);
    const dataBytes = r.bytes(dataLen);
    const offsetsBytes = r.bytes(offsetsLen);
    const extraBytes = r.bytes(extraLen);

    const baseName = typeNameFromCode(typeCode);
    let payload: string[];

    try {
        payload = decodeByStrategy(baseName, encoding, flags, rowCount, dataBytes, offsetsBytes, extraBytes);
    } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        throw new Error(`RBCF: column '${name}' decode failed: ${msg}`);
    }

    // Overlay nones bitmap: absent cells become NONE_VALUE sentinel (matches JSON path).
    if (hasNones && nonesLen > 0) {
        const bits = decodeBitvec(nonesBytes, rowCount);
        for (let i = 0; i < rowCount; i++) {
            if (!bits[i]) payload[i] = NONE_VALUE;
        }
    }

    const finalType: Type = optionOuter ? { Option: baseName as Type } : (baseName as Type);
    return { name, type: finalType, payload };
}

function decodeByStrategy(
    typeName: ReturnType<typeof typeNameFromCode>,
    encoding: ColumnEncoding,
    flags: number,
    rowCount: number,
    data: Uint8Array,
    offsets: Uint8Array,
    extra: Uint8Array
): string[] {
    switch (encoding) {
        case ColumnEncoding.Plain:
        case ColumnEncoding.BitPack:
            return decodePlain(typeName, rowCount, data, offsets);
        case ColumnEncoding.Dict:
            return decodeDict(typeName, rowCount, data, extra, dictIndexWidthFromFlags(flags));
        case ColumnEncoding.Rle:
            return decodeRle(typeName, rowCount, data);
        case ColumnEncoding.Delta:
            return decodeDelta(typeName, rowCount, data);
        case ColumnEncoding.DeltaRle:
            return decodeDeltaRle(typeName, rowCount, data);
        default:
            throw new Error(`RBCF: unknown encoding ${encoding}`);
    }
}
