// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Int4", () => {
    it("round-trips signed 32-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Int4", payload: ["-2147483648", "-1", "0", "1", "2147483647"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Int4> with nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: { Option: "Int4" }, payload: ["1", "⟪none⟫", "3"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
