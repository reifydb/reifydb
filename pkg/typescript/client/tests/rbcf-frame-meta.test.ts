// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf, type WireFrame } from "../src/rbcf";
import { MESSAGE_HEADER_SIZE } from "../src/rbcf/format";

const META_BYTE_AT = MESSAGE_HEADER_SIZE + 6;

function rownumFrame(): WireFrame {
    return {
        columns: [
            { name: "#rownum", type: "Uint8", payload: ["1", "2"] },
            { name: "id", type: "Int4", payload: ["10", "20"] },
        ],
    };
}

describe("rbcf frame meta byte", () => {
    it("writes a zero meta byte for a frame that carries a #rownum column", () => {
        // system columns ride as ordinary columns, so encode must never flag a side section
        expect(rbcf.encode([rownumFrame()])[META_BYTE_AT]).toBe(0);
    });

    it("round trips #rownum as an ordinary column", () => {
        // without a side section the #rownum column must survive decode exactly as sent
        expect(rbcf.decode(rbcf.encode([rownumFrame()]))).toEqual([rownumFrame()]);
    });

    it("rejects a non-zero meta byte", () => {
        // a flagged side section must fail loud, never be read as the first column
        const bytes = rbcf.encode([rownumFrame()]);
        bytes[META_BYTE_AT] = 1;
        expect(() => rbcf.decode(bytes)).toThrow("RBCF: frame meta byte must be 0, found 0x01");
    });
});
