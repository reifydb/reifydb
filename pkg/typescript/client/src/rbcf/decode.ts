// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { FixedPointKind, FixedPointType, Type } from "@reifydb/core";
import { FIXED_POINT_NARROW_PRECISION, fixedPointType, noneMarker } from "@reifydb/core";

import {
    COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, ColumnEncoding, FRAME_HEADER_SIZE,
    META_HAS_CREATED_AT, META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT, MESSAGE_HEADER_SIZE,
    RBCF_MAGIC, RBCF_VERSION, TAG_DEPTH_SHIFT, TAG_KIND_MASK, TYPE_CODE, dictIndexWidthFromFlags, typeNameFromCode,
} from "./format";
import { BinaryReader } from "./reader";
import { decodeBitvec } from "./nones";
import { digitCount, formatDateTime, formatFixedPoint } from "./values";
import type { WireColumn, WireFrame } from "./types";
import { decodeDigestPlain, decodePlain, decodePlainFixedPoint } from "./encoding/plain";
import { decodeDict } from "./encoding/dict";
import { decodeRle, decodeRleFixedPoint } from "./encoding/rle";
import { decodeDelta, decodeDeltaFixedPoint, decodeDeltaRle } from "./encoding/delta";

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
    r.u16();
    const frameCount = r.u32();
    r.u32();

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
    const op = r.u8();
    r.u32();
    void frameStart;

    const frame: WireFrame = { columns: [] };
    if (op === 1 || op === 2 || op === 3) frame.op = op;
    else if (op !== 0) throw new Error(`RBCF: unknown frame op ${op}`);

    if (metaFlags & META_HAS_ROW_NUMBERS) {
        const rows = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) rows[i] = r.u64().toString();
        frame.row_numbers = rows;
    }
    if (metaFlags & META_HAS_CREATED_AT) {
        const cr = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) cr[i] = formatDateTime(r.i64());
        frame.created_at = cr;
    }
    if (metaFlags & META_HAS_UPDATED_AT) {
        const up = new Array<string>(rowCount);
        for (let i = 0; i < rowCount; i++) up[i] = formatDateTime(r.i64());
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
    r.u8();
    const nameLen = r.u16();
    r.u16();
    const rowCount = r.u32();
    const nonesLen = r.u32();
    const dataLen = r.u32();
    const offsetsLen = r.u32();
    const extraLen = r.u32();

    const encoding = encodingByte as ColumnEncoding;
    const hasNones = (flags & COL_FLAG_HAS_NONES) !== 0;
    const depth = typeCode >> TAG_DEPTH_SHIFT;

    const name = r.utf8(nameLen);
    const namePad = (4 - (nameLen % 4)) % 4;
    if (namePad > 0) r.skip(namePad);

    const nonesBytes = r.bytes(nonesLen);
    const dataBytes = r.bytes(dataLen);
    const offsetsBytes = r.bytes(offsetsLen);
    const extraBytes = r.bytes(extraLen);

    const bitmapLen = (rowCount + 7) >> 3;
    if (nonesLen !== depth * bitmapLen) {
        throw new Error(
            `RBCF: column '${name}' nones length ${nonesLen} disagrees with option depth ${depth} ` +
            `and row count ${rowCount} (expected ${depth * bitmapLen})`
        );
    }
    if (hasNones !== depth > 0) {
        throw new Error(`RBCF: column '${name}' has-nones flag disagrees with option depth ${depth}`);
    }

    const kind = typeCode & TAG_KIND_MASK;
    const baseName = typeNameFromCode(kind);
    let type: Type = baseName as Type;
    let payload: string[];

    try {
        if (kind === TYPE_CODE.Digest) {
            if (encoding !== ColumnEncoding.Plain) {
                throw new Error(`digest column must use plain encoding, found ${ColumnEncoding[encoding] ?? encoding}`);
            }
            const digest = decodeDigestPlain(rowCount, dataBytes, offsetsBytes, extraBytes);
            type = digest.type;
            payload = digest.payload;
        } else if (kind === TYPE_CODE.Int || kind === TYPE_CODE.Uint || kind === TYPE_CODE.Decimal) {
            const fixed = decodeFixedPointColumn(baseName as FixedPointKind, encoding, rowCount, dataBytes, extraBytes);
            type = fixed.type;
            payload = fixed.payload;
        } else {
            payload = decodeByStrategy(baseName, encoding, flags, rowCount, dataBytes, offsetsBytes, extraBytes);
        }
    } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        throw new Error(`RBCF: column '${name}' decode failed: ${msg}`);
    }

    const defined = new Array<boolean>(rowCount).fill(true);
    for (let layer = 0; layer < depth; layer++) {
        const bits = decodeBitvec(nonesBytes.subarray(layer * bitmapLen, (layer + 1) * bitmapLen), rowCount);
        const marker = noneMarker(layer);
        for (let i = 0; i < rowCount; i++) {
            if (!defined[i] || bits[i]) continue;
            defined[i] = false;
            payload[i] = marker;
        }
        type = { Option: type };
    }

    return { name, type, payload };
}

function decodeFixedPointColumn(
    kind: FixedPointKind,
    encoding: ColumnEncoding,
    rowCount: number,
    data: Uint8Array,
    extra: Uint8Array
): { type: FixedPointType; payload: string[] } {
    if (extra.length !== 2) {
        throw new Error(`${kind} column needs precision and scale in 2 extra bytes, found ${extra.length}`);
    }
    const [precision, scale] = extra;
    const type = fixedPointType(kind, precision, scale);
    const width = precision <= FIXED_POINT_NARROW_PRECISION ? 16 : 32;
    let values: bigint[];
    switch (encoding) {
        case ColumnEncoding.Plain:
            values = decodePlainFixedPoint(rowCount, data, width);
            break;
        case ColumnEncoding.Rle:
            values = decodeRleFixedPoint(rowCount, data, width);
            break;
        case ColumnEncoding.Delta:
            values = decodeDeltaFixedPoint(rowCount, data, width, false);
            break;
        case ColumnEncoding.DeltaRle:
            values = decodeDeltaFixedPoint(rowCount, data, width, true);
            break;
        default:
            throw new Error(`${ColumnEncoding[encoding] ?? encoding} encoding not supported for type ${kind}`);
    }
    for (const value of values) {
        if (digitCount(value) > precision) {
            throw new Error(`${kind} value ${value} has more digits than precision ${precision}`);
        }
        if (kind === "Uint" && value < 0n) throw new Error(`Uint value ${value} is negative`);
    }
    return { type, payload: values.map((value) => formatFixedPoint(value, scale)) };
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
