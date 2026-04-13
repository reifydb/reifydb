// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Uint16", () => {
    it("round-trips unsigned 128-bit boundaries as bigint strings", () => {
        const frames: WireFrame[] = [
            {
                columns: [
                    {
                        name: "v",
                        type: "Uint16",
                        payload: ["0", "1", "340282366920938463463374607431768211455"],
                    },
                ],
            },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
