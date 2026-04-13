// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, expect, it } from "vitest";
import { rbcf } from "../../src/rbcf";
import type { WireFrame } from "../../src/rbcf";

describe("rbcf Uint2", () => {
    it("round-trips unsigned 16-bit boundaries", () => {
        const frames: WireFrame[] = [
            { columns: [{ name: "v", type: "Uint2", payload: ["0", "1", "32767", "65535"] }] },
        ];
        expect(rbcf.decode(rbcf.encode(frames))).toEqual(frames);
    });
});
