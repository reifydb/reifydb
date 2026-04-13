// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Float8", () => {
    it("round-trips typical values", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Float8", payload: ["0", "1.5", "-3.25", "2.718281828459045"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips NaN / infinities", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Float8", payload: ["NaN", "inf", "-inf"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
