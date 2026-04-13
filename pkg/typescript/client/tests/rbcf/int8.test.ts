// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Int8", () => {
    it("round-trips signed 64-bit boundaries as bigint strings", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    {
                        name: "v",
                        type: "Int8",
                        payload: ["-9223372036854775808", "-1", "0", "1", "9223372036854775807"],
                    },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });

    it("round-trips Option<Int8> with nones", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    { name: "v", type: { Option: "Int8" }, payload: ["⟪none⟫", "42", "⟪none⟫"] },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
