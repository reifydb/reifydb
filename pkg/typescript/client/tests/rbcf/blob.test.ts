// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Blob", () => {
    it("round-trips lowercase 0x-prefixed hex bytes", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "b", type: "Blob", payload: ["0x", "0xdeadbeef", "0x00", "0x00ff"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Blob> with nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "b", type: { Option: "Blob" }, payload: ["0xdead", "⟪none⟫"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
