// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { NONE_VALUE, columnsFromWire } from "@reifydb/core";
import { rbcf } from "../src/rbcf";
import {
    COL_FLAG_HAS_NONES, ColumnEncoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE,
    RBCF_MAGIC, RBCF_VERSION, TAG_DEPTH_SHIFT, TYPE_CODE,
} from "../src/rbcf/format";
import { encodeBitvec } from "../src/rbcf/nones";
import { BinaryWriter } from "../src/rbcf/writer";

function enumMessage(enumName: string, names: string[], nones?: boolean[]): Uint8Array {
    // Kind 31 is laid out like Utf8 with the qualified enum name in front of the extra section.
    const data = new BinaryWriter();
    const offsets = new BinaryWriter();
    let end = 0;
    offsets.u32(0);
    for (const v of names) {
        end += data.utf8(v);
        offsets.u32(end);
    }
    const dataBytes = data.finish().slice();
    const offsetBytes = offsets.finish().slice();
    const nameBytes = new TextEncoder().encode(enumName);
    const extra = new Uint8Array(2 + nameBytes.length);
    new DataView(extra.buffer).setUint16(0, nameBytes.length, true);
    extra.set(nameBytes, 2);
    const depth = nones ? 1 : 0;
    const noneBytes = nones ? encodeBitvec(nones) : new Uint8Array(0);

    const w = new BinaryWriter();
    const messageAt = w.reserve(MESSAGE_HEADER_SIZE);
    const frameAt = w.reserve(FRAME_HEADER_SIZE);
    const columnName = new TextEncoder().encode("status");
    w.u8((depth << TAG_DEPTH_SHIFT) | TYPE_CODE.Enum);
    w.u8(ColumnEncoding.Plain);
    w.u8(nones ? COL_FLAG_HAS_NONES : 0);
    w.u8(0);
    w.u16(columnName.length);
    w.u16(0);
    w.u32(names.length);
    w.u32(noneBytes.length);
    w.u32(dataBytes.length);
    w.u32(offsetBytes.length);
    w.u32(extra.length);
    w.bytes(columnName);
    w.zeroes((4 - (columnName.length % 4)) % 4);
    w.bytes(noneBytes);
    w.bytes(dataBytes);
    w.bytes(offsetBytes);
    w.bytes(extra);

    w.patchU32(frameAt, names.length);
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

describe("RBCF enum columns", () => {
    it("decode kind 31 as Utf8 names with the enum marker", () => {
        // D3: clients read plain names; a missing marker loses the enum type, a tag here means the old layout.
        const [frame] = rbcf.decode(enumMessage("s::status", ["todo", "doing"]));
        expect(frame.columns[0]).toEqual({ name: "status", type: "Utf8", payload: ["todo", "doing"], enum: "s::status" });
    });

    it("keep none rows of an optional enum as none", () => {
        // A none tag must stay none, not become an empty name.
        const [frame] = rbcf.decode(enumMessage("s::status", ["", "done"], [false, true]));
        expect(frame.columns[0]).toEqual({
            name: "status", type: { Option: "Utf8" }, payload: [NONE_VALUE, "done"], enum: "s::status",
        });
    });

    it("refuse a kind 31 column whose name is cut short", () => {
        // A truncated name must fail loud, otherwise the rest of the extra is read as a wrong name.
        const bytes = enumMessage("s::status", ["todo"]);
        const extraLenAt = MESSAGE_HEADER_SIZE + FRAME_HEADER_SIZE + 24;
        new DataView(bytes.buffer, bytes.byteOffset).setUint32(extraLenAt, 1, true);
        expect(() => rbcf.decode(bytes)).toThrow();
    });

    it("round trip the marker through the TS encoder", () => {
        // Encode must write what decode reads, otherwise a forwarded frame drops the enum type.
        const column = { name: "status", type: "Utf8" as const, payload: ["todo"], enum: "s::status" };
        const [frame] = rbcf.decode(rbcf.encode([{ columns: [column] }]));
        expect(frame.columns[0]).toEqual(column);
    });
});

describe("json enum columns", () => {
    it("read the enum descriptor as Utf8 and keep the qualified name", () => {
        // D3: `{"id":"Enum","sumtype":"ns::name"}` with names as payload, under Option when nullable.
        const [plain, optional] = columnsFromWire([
            { name: "a", type: { id: "Enum", sumtype: "s::status" }, payload: ["doing"] },
            { name: "b", type: { id: "Option", underlying: { id: "Enum", sumtype: "s::status" } }, payload: ["done"] },
        ]);
        expect(plain).toEqual({ name: "a", type: "Utf8", payload: ["doing"], enum: "s::status" });
        expect(optional).toEqual({ name: "b", type: { Option: "Utf8" }, payload: ["done"], enum: "s::status" });
    });

    it("refuse an enum descriptor with a numeric sumtype", () => {
        // D1: an id is no longer part of the wire, so the old descriptor must be refused, not half read.
        expect(() => columnsFromWire([{ name: "a", type: { id: "Enum", sumtype: 7 } as any, payload: ["1"] }])).toThrow();
    });
});
