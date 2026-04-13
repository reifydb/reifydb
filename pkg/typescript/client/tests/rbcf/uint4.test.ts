// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Uint4", () => {
    it("round-trips unsigned 32-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Uint4", payload: ["0", "1", "2147483647", "4294967295"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
