// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Int1", () => {
    it("round-trips signed 8-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Int1", payload: ["-128", "-1", "0", "1", "127"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Int1> with nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: { Option: "Int1" }, payload: ["⟪none⟫", "7", "⟪none⟫"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
