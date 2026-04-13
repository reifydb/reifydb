// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Int2", () => {
    it("round-trips signed 16-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Int2", payload: ["-32768", "-1", "0", "1", "32767"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Int2> with nones", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: { Option: "Int2" }, payload: ["⟪none⟫", "100"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
