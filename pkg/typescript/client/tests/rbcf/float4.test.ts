// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Float4", () => {
    it("round-trips integer-valued floats", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Float4", payload: ["0", "1", "-3", "42"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips NaN / infinities", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Float4", payload: ["NaN", "inf", "-inf"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
