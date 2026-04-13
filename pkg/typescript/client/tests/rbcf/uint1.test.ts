// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Uint1", () => {
    it("round-trips unsigned 8-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Uint1", payload: ["0", "127", "128", "255"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
