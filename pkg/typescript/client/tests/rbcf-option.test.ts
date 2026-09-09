// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { NONE_VALUE } from "@reifydb/core";
import { rbcf, type WireFrame } from "../src/rbcf";
import {
    COL_FLAG_HAS_NONES, ColumnEncoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE,
    RBCF_MAGIC, RBCF_VERSION, TAG_DEPTH_SHIFT, TYPE_CODE, type TypeName,
} from "../src/rbcf/format";
import { encodeBitvec } from "../src/rbcf/nones";
import { BinaryWriter } from "../src/rbcf/writer";

const SOME_NONE = `${NONE_VALUE.slice(0, -1)}:1${NONE_VALUE.slice(-1)}`;

interface ColumnSpec {
    name: string;
    base: TypeName;
    depth: number;
    rows: number;
    flags: number;
    nones: Uint8Array;
    data: Uint8Array;
    offsets: Uint8Array;
}

function bitmaps(layers: boolean[][]): Uint8Array {
    const parts = layers.map(encodeBitvec);
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let pos = 0;
    for (const p of parts) {
        out.set(p, pos);
        pos += p.length;
    }
    return out;
}

function int4Data(values: number[]): Uint8Array {
    const w = new BinaryWriter();
    for (const v of values) w.i32(v);
    return w.finish().slice();
}

function utf8Data(values: string[]): { data: Uint8Array; offsets: Uint8Array } {
    const data = new BinaryWriter();
    const offsets = new BinaryWriter();
    let end = 0;
    offsets.u32(0);
    for (const v of values) {
        end += data.utf8(v);
        offsets.u32(end);
    }
    return { data: data.finish().slice(), offsets: offsets.finish().slice() };
}

function message(col: ColumnSpec): Uint8Array {
    const w = new BinaryWriter();
    const messageAt = w.reserve(MESSAGE_HEADER_SIZE);
    const frameAt = w.reserve(FRAME_HEADER_SIZE);

    const nameBytes = new TextEncoder().encode(col.name);
    w.u8((col.depth << TAG_DEPTH_SHIFT) | TYPE_CODE[col.base]);
    w.u8(ColumnEncoding.Plain);
    w.u8(col.flags);
    w.u8(0);
    w.u16(nameBytes.length);
    w.u16(0);
    w.u32(col.rows);
    w.u32(col.nones.length);
    w.u32(col.data.length);
    w.u32(col.offsets.length);
    w.u32(0);
    w.bytes(nameBytes);
    w.zeroes((4 - (nameBytes.length % 4)) % 4);
    w.bytes(col.nones);
    w.bytes(col.data);
    w.bytes(col.offsets);

    w.patchU32(frameAt, col.rows);
    w.patchU16(frameAt + 4, 1);
    w.patchU8(frameAt + 6, 0);
    w.patchU8(frameAt + 7, 0);
    w.patchU32(frameAt + 8, w.length - frameAt);

    w.patchU32(messageAt, RBCF_MAGIC);
    w.patchU16(messageAt + 4, RBCF_VERSION);
    w.patchU16(messageAt + 6, 0);
    w.patchU32(messageAt + 8, 1);
    w.patchU32(messageAt + 12, w.length);
    return w.finish().slice();
}

function decodeOne(col: ColumnSpec) {
    const frames = rbcf.decode(message(col));
    expect(frames).toHaveLength(1);
    expect(frames[0].columns).toHaveLength(1);
    return frames[0].columns[0];
}

describe("RBCF option columns", () => {
    it("depth zero decodes as a plain column", () => {
        const col = decodeOne({
            name: "n", base: "Int4", depth: 0, rows: 2, flags: 0,
            nones: new Uint8Array(0), data: int4Data([1, 2]), offsets: new Uint8Array(0),
        });
        expect(col).toEqual({ name: "n", type: "Int4", payload: ["1", "2"] });
    });

    it("depth one fixed-width: None and Some(v)", () => {
        const col = decodeOne({
            name: "n", base: "Int4", depth: 1, rows: 2, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[false, true]]), data: int4Data([0, 7]), offsets: new Uint8Array(0),
        });
        expect(col).toEqual({ name: "n", type: { Option: "Int4" }, payload: [NONE_VALUE, "7"] });
    });

    it("depth one varlen: None and Some(v)", () => {
        const { data, offsets } = utf8Data(["", "ab"]);
        const col = decodeOne({
            name: "s", base: "Utf8", depth: 1, rows: 2, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[false, true]]), data, offsets,
        });
        expect(col).toEqual({ name: "s", type: { Option: "Utf8" }, payload: [NONE_VALUE, "ab"] });
    });

    it("depth two fixed-width: None, Some(None) and Some(Some(v))", () => {
        // The inner bit under the outer None is cleared too: it must not turn that row into Some(None).
        const col = decodeOne({
            name: "n", base: "Int4", depth: 2, rows: 3, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[false, true, true], [false, false, true]]),
            data: int4Data([0, 0, 7]), offsets: new Uint8Array(0),
        });
        expect(col).toEqual({
            name: "n",
            type: { Option: { Option: "Int4" } },
            payload: [NONE_VALUE, SOME_NONE, "7"],
        });
    });

    it("depth two varlen: None, Some(None) and Some(Some(v))", () => {
        const { data, offsets } = utf8Data(["", "", "ab"]);
        const col = decodeOne({
            name: "s", base: "Utf8", depth: 2, rows: 3, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[false, true, true], [true, false, true]]), data, offsets,
        });
        expect(col).toEqual({
            name: "s",
            type: { Option: { Option: "Utf8" } },
            payload: [NONE_VALUE, SOME_NONE, "ab"],
        });
    });

    it("depth two slices one ceil(rows / 8) bitmap per layer beyond eight rows", () => {
        const rows = 9;
        const outer = new Array<boolean>(rows).fill(true);
        const inner = new Array<boolean>(rows).fill(true);
        outer[8] = false;
        inner[0] = false;
        const values = Array.from({ length: rows }, (_, i) => i);
        const col = decodeOne({
            name: "n", base: "Int4", depth: 2, rows, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([outer, inner]), data: int4Data(values), offsets: new Uint8Array(0),
        });
        expect(col.payload).toEqual([SOME_NONE, "1", "2", "3", "4", "5", "6", "7", NONE_VALUE]);
    });

    it("fully populated option column keeps its option type", () => {
        const col = decodeOne({
            name: "n", base: "Int4", depth: 1, rows: 2, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[true, true]]), data: int4Data([1, 2]), offsets: new Uint8Array(0),
        });
        expect(col).toEqual({ name: "n", type: { Option: "Int4" }, payload: ["1", "2"] });
    });

    it("rejects a nones length that disagrees with depth and row count", () => {
        const bytes = message({
            name: "broken", base: "Int4", depth: 2, rows: 3, flags: COL_FLAG_HAS_NONES,
            nones: bitmaps([[true, true, true]]), data: int4Data([1, 2, 3]), offsets: new Uint8Array(0),
        });
        expect(() => rbcf.decode(bytes)).toThrow(/column 'broken'.*nones length 1.*depth 2/);
    });

    it("rejects a has-nones flag that disagrees with depth", () => {
        const bytes = message({
            name: "flagged", base: "Int4", depth: 0, rows: 1, flags: COL_FLAG_HAS_NONES,
            nones: new Uint8Array(0), data: int4Data([1]), offsets: new Uint8Array(0),
        });
        expect(() => rbcf.decode(bytes)).toThrow(/column 'flagged'.*has-nones flag/);
    });
});

describe("RBCF option columns round trip", () => {
    function roundTrip(frame: WireFrame): WireFrame {
        const frames = rbcf.decode(rbcf.encode([frame]));
        expect(frames).toHaveLength(1);
        return frames[0];
    }

    it("depth zero column is unchanged", () => {
        const frame: WireFrame = {
            columns: [
                { name: "n", type: "Int4", payload: ["1", "2", "3"] },
                { name: "s", type: "Utf8", payload: ["", "ab", "c"] },
            ],
        };
        expect(roundTrip(frame)).toEqual(frame);
    });

    it("depth one: None and Some(v) for fixed-width and varlen", () => {
        const frame: WireFrame = {
            columns: [
                { name: "n", type: { Option: "Int4" }, payload: [NONE_VALUE, "7", NONE_VALUE] },
                { name: "s", type: { Option: "Utf8" }, payload: ["ab", NONE_VALUE, "c"] },
            ],
        };
        expect(roundTrip(frame)).toEqual(frame);
    });

    it("depth two: None, Some(None) and Some(Some(v)) for fixed-width and varlen", () => {
        const frame: WireFrame = {
            columns: [
                { name: "n", type: { Option: { Option: "Int4" } }, payload: [NONE_VALUE, SOME_NONE, "7"] },
                { name: "s", type: { Option: { Option: "Utf8" } }, payload: ["ab", NONE_VALUE, SOME_NONE] },
            ],
        };
        expect(roundTrip(frame)).toEqual(frame);
    });

    it("depth two writes one bitmap per layer beyond eight rows", () => {
        const payload = Array.from({ length: 9 }, (_, i) => String(i));
        payload[0] = SOME_NONE;
        payload[8] = NONE_VALUE;
        const frame: WireFrame = { columns: [{ name: "n", type: { Option: { Option: "Int4" } }, payload }] };
        expect(roundTrip(frame)).toEqual(frame);
    });

    it("fully populated option column keeps its option type", () => {
        const frame: WireFrame = { columns: [{ name: "n", type: { Option: "Int4" }, payload: ["1", "2"] }] };
        expect(roundTrip(frame)).toEqual(frame);
    });

    it("rejects a none deeper than the column's option depth", () => {
        const frame: WireFrame = { columns: [{ name: "n", type: { Option: "Int4" }, payload: [SOME_NONE] }] };
        expect(() => rbcf.encode([frame])).toThrow(/column 'n' none under 1 Some layers.*depth 1/);
    });
});
